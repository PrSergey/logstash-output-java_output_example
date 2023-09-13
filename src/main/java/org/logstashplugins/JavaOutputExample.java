package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.PluginConfigSpec;
import com.google.common.collect.ImmutableMap;
import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.*;

import java.io.OutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

// class name must match plugin name
@LogstashPlugin(name = "java_output_example")
public class JavaOutputExample implements Output {

    public static final PluginConfigSpec<String> CONNECTION_STRING =
            PluginConfigSpec.stringSetting("connection_string", "");

    public static final PluginConfigSpec<String> SA_KEY_FILE =
            PluginConfigSpec.stringSetting("sa_key_file", "");

    public static final PluginConfigSpec<String> TABLE_NAME =
            PluginConfigSpec.stringSetting("table", "logstash", false, true);

    public static final PluginConfigSpec<List<Object>> COLUMNS =
            PluginConfigSpec.arraySetting("column");


    public static final StructType MESSAGE_TYPE = StructType.of(
            "id", PrimitiveType.Text,
            "timestamp", PrimitiveType.Text,
            "text", PrimitiveType.Text
    );


    private final String id;
    private PrintStream printer;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped = false;
    private GrpcTransport transport;
    private TableClient tableClient;
    private String database;
    private SessionRetryContext retryCtx;
    private String tableName;
    private String connectionString;
    private String saKeyFile;
    private Column[] columns;


    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public JavaOutputExample(final String id, final Configuration configuration, final Context context) {
        this(id, configuration, context, System.out);
    }

    JavaOutputExample(final String id, final Configuration config, final Context context, OutputStream targetStream) {
        // constructors should validate configuration options
        this.id = id;
        printer = new PrintStream(targetStream);
        tableName = config.get(TABLE_NAME);
        connectionString = config.get(CONNECTION_STRING);
        tableName = config.get(TABLE_NAME);
        saKeyFile = config.get(SA_KEY_FILE);
        createSession();
        createTable();
    }

    private void createSession() {
        AuthProvider authProvider = CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);
        transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build();
        tableClient = TableClient
                    .newClient(transport)
                    .build();
        this.database = transport.getDatabase();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }


    private void createTable() {
        TableDescription logstashTable = TableDescription.newBuilder()
                .addNonnullColumn("id", PrimitiveType.Text)
                .addNullableColumn("timestamp", PrimitiveType.Text)
                .addNullableColumn("text", PrimitiveType.Text)
                .setPrimaryKey("id")
                .build();
        retryCtx.supplyStatus(session -> session.createTable(database + "/" + tableName, logstashTable))
                .join()
                .expectSuccess("Can't create table /" + tableName);
    }

    @Override
    public void output(final Collection<Event> events) {
        List<Message> allEvents = new ArrayList<>();
        Iterator<Event> z = events.iterator();
        while (z.hasNext() && !stopped) {
            Event event = z.next();
            Instant eventTimestamp = event.getEventTimestamp();
            String text = event.getField("message").toString();
            UUID uuid = UUID.randomUUID();
            Message message = new Message(uuid.toString(), text, eventTimestamp);
            allEvents.add(message);
            printer.println(event);
        }
        upsertInDb(allEvents);
    }

    private void upsertInDb(List<Message> allEvents) {
        if (allEvents.isEmpty()) {
            return;
        }
        String query = "declare $values as List<Struct<id:Utf8,timestamp:Utf8,text:Utf8>>;" +
                "upsert into " + tableName + " select * from as_table($values);";
        TxControl txControl = TxControl.serializableRw().setCommitTx(true);
        ListValue messages = createMessagesValue(allEvents);
        Params params = Params.of("$values", messages);
        DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl, params))
                .join().getValue();

    }

    @Override
    public void stop() {
        tableClient.close();
        transport.close();
        stopped = true;
        done.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return new ArrayList<>(List.of(CONNECTION_STRING, SA_KEY_FILE, TABLE_NAME));
    }

    @Override
    public String getId() {
        return id;
    }

    private ListValue createMessagesValue(List<Message> messages) {
        return toListValue(messages, MESSAGE_TYPE, s -> ImmutableMap.of(
                "id", PrimitiveValue.newText(s.getId()),
                "timestamp", PrimitiveValue.newText(s.getTimestamp().toString()),
                "text", PrimitiveValue.newText(s.getText())));
    }

    private ListValue toListValue(List<Message> messages, StructType type, Function<Message, Map<String, Value<?>>> mapper) {
        ListType listType = ListType.of(type);
        return listType.newValue(messages.stream()
                .map(e -> type.newValue(mapper.apply(e)))
                .collect(Collectors.toList()));
    }
}

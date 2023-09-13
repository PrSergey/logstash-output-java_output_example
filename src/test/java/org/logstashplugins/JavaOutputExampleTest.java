package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Event;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstashplugins.JavaOutputExample;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class JavaOutputExampleTest {

    @Test
    public void testJavaOutputExample() {
        String connectionString = "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1glvg8scoq6ppp1u1f3/etn9r2rtl5dos3k5dhg8";
        String saKeyFile = "C:/Users/provo/dev/ydb/authorized_key.json";

        String tableName = "test_table";
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(JavaOutputExample.CONNECTION_STRING.name(), connectionString);
        configValues.put(JavaOutputExample.TABLE_NAME.name(), tableName);
        configValues.put(JavaOutputExample.SA_KEY_FILE.name(), saKeyFile);
        Configuration config = new ConfigurationImpl(configValues);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JavaOutputExample output = new JavaOutputExample("id", config, null, baos);

        String sourceField = "message";
        int eventCount = 5;
        Collection<Event> events = new ArrayList<>();
        for (int k = 0; k < eventCount; k++) {
            Event e = new org.logstash.Event();
            e.setField(sourceField, "message " + k);
            events.add(e);
        }

        output.output(events);

        String outputString = baos.toString();
        int index = 0;
        int lastIndex = 0;
        while (index < eventCount) {
            lastIndex = outputString.indexOf("message", lastIndex);
            Assert.assertTrue("Prefix should exist in output string", lastIndex > -1);
            lastIndex = outputString.indexOf("message " + index);
            Assert.assertTrue("Message should exist in output string", lastIndex > -1);
            index++;
        }
    }
}

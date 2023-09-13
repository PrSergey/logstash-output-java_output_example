package org.logstashplugins;

import java.time.Instant;
import java.util.Objects;


public class Message {


    private String id;
    private String text;
    private Instant timestamp;

    public Message(String id, String text, Instant timestamp) {
        this.id = id;
        this.text = text;
        this.timestamp = timestamp;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", text='" + text + '\'' +
                ", time=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return Objects.equals(id, message.id) && Objects.equals(text, message.text) && Objects.equals(timestamp, message.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, text, timestamp);
    }
}

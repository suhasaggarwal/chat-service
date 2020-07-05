package org.wayneyu.chat.model;

import java.util.Objects;

public class Message {

    private int index;
    private long timestamp;
    private String author;
    private String message;

    public Message(int index, long timestamp, String author, String message) {
        this.index = index;
        this.timestamp = timestamp;
        this.author = author;
        this.message = message;
    }

    public int getIndex() {
        return index;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getAuthor() {
        return author;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return String.format("%d, [%d] %s: %s", index, timestamp, author, message);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message1 = (Message) o;
        return index == message1.index &&
                timestamp == message1.timestamp &&
                Objects.equals(author, message1.author) &&
                Objects.equals(message, message1.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, timestamp, author, message);
    }
}

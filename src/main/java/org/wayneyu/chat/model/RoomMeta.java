package org.wayneyu.chat.model;

public class RoomMeta {
    private long messageCount;
    private long created;
    private long lastMessageTimestamp;

    public RoomMeta(long messageCount, long created, long lastMessageTimestamp){
        this.messageCount = messageCount;
        this.created = created;
        this.lastMessageTimestamp = lastMessageTimestamp;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public long getCreated() {
        return created;
    }

    public long getLastMessageTimestamp() {
        return lastMessageTimestamp;
    }
}

package org.wayneyu.chat.model;

import java.util.List;

public class Messages {

    private int chatRoomId;
    private List<Message> messages;

    public Messages(int chatRoomId, List<Message> messages) {
        this.chatRoomId = chatRoomId;
        this.messages = messages;
    }

    public int getChatRoomId() {
        return chatRoomId;
    }

    public List<Message> getMessages() {
        return messages;
    }
}

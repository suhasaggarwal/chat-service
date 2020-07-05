package org.wayneyu.chat.db;

import org.wayneyu.chat.model.Message;
import org.wayneyu.chat.model.Room;

import java.util.List;

public interface ChatRepository {

    void createRoom(Room room);

    void addMessages(int chatRoomId, List<Message> messages);

    List<Message> getMessages(int chatRoomId, long startTime, long endTime);

    Room getRoom(int id);

    int countLongPauses(int chatRoomId, long startTime, long endTime);

}
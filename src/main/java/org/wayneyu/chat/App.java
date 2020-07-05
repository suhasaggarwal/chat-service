package org.wayneyu.chat;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wayneyu.chat.db.ChatRepository;
import org.wayneyu.chat.db.HBaseRepositoryImpl;
import org.wayneyu.chat.model.Message;
import org.wayneyu.chat.model.Messages;
import org.wayneyu.chat.model.Room;
import org.wayneyu.chat.model.RoomMeta;

import java.util.Arrays;
import java.util.List;

import static com.sun.org.apache.xml.internal.serializer.utils.Utils.messages;
import static spark.Spark.*;

public class App {

    private static Logger log = LoggerFactory.getLogger(App.class);

    private static final String ROOM_TABLE_NAME = "ROOM";
    private static final String MESSAGE_TABLE_NAME = "MESSAGE";
    private static final ChatRepository repo = new HBaseRepositoryImpl(ROOM_TABLE_NAME, MESSAGE_TABLE_NAME);
    private static final String acceptType = "application/json";

    private static Gson gson = new Gson();

    public static void main(String[] args) {
        port(9999);

        put("/room", acceptType, (request, response) -> {
            Room room = gson.fromJson(request.body(), Room.class);
            repo.createRoom(room);
            return "created";
        }, gson::toJson);

        get("/room/:id", (request, response) -> {
            Room room = repo.getRoom(Integer.parseInt(request.params("id")));
            if (room != null) {
                return room;
            } else {
                response.status(404);
                return "Room not found";
            }
        }, gson::toJson);

        put("/messages", acceptType, (request, response) -> {
            Messages messages = gson.fromJson(request.body(), Messages.class);
            repo.addMessages(messages.getChatRoomId(), messages.getMessages());
            return "added";
        });

        get("/room/:roomId/messages/:start/:end", (request, response) -> {
            int roomId = Integer.parseInt(request.params("roomId"));
            long start = Long.parseLong(request.params("start"));
            long end = Long.parseLong(request.params("end"));
            List<Message> messages = repo.getMessages(roomId, start, end);
            return messages;
        }, gson::toJson);

        get("/room/:roomId/long-pauses/:start/:end", (request, response) -> {
            int roomId = Integer.parseInt(request.params("roomId"));
            long start = Long.parseLong(request.params("start"));
            long end = Long.parseLong(request.params("end"));
            int longPausesCount = repo.countLongPauses(roomId, start, end);
            return longPausesCount;
        });
    }
}

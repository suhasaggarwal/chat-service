package org.wayneyu.chat.db;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wayneyu.chat.hbase.HBaseService;
import org.wayneyu.chat.model.Message;
import org.wayneyu.chat.model.Room;
import org.wayneyu.chat.model.RoomMeta;

import java.io.IOException;
import java.util.*;

public class HBaseRepositoryImpl implements ChatRepository {

    private static final Logger logger = LoggerFactory.getLogger(HBaseRepositoryImpl.class);

    private static HBaseService hbase;

    public static final String ROOM_INFO_COLUMN_FAMILY = "info";
    public static final String ROOM_META_COLUMN_FAMILY = "meta";
    public static final String MESSAGE_MESSAGE_COLUMN_FAMILY = "message";

    private String roomTableName;
    private String messageTableName;

    static {
        hbase = new HBaseService();
        hbase.init();
    }

    public HBaseRepositoryImpl(String roomTableName, String messageTableName) {
        this.roomTableName = roomTableName;
        this.messageTableName = messageTableName;
        createTables();
    }

    private void createTables() {
        try {
            hbase.createTable(TableName.valueOf(roomTableName), new String[]{ROOM_INFO_COLUMN_FAMILY, ROOM_META_COLUMN_FAMILY});
            hbase.createTable(TableName.valueOf(messageTableName), new String[]{MESSAGE_MESSAGE_COLUMN_FAMILY});
        } catch (IOException e) {
            logger.error("Failed to initialize HBase", e);
            throw new RuntimeException(e);
        }
    }

    private String getRoomRowkey(int roomId) {
        return Integer.toString(roomId);
    }

    private String getMessageRowkey(int roomId, Message message) {
        return getMessageRowkey(roomId, message.getTimestamp());
    }

    private String getMessageRowkey(int roomId, long timestamp) {
        return String.format("%010d_%013d", roomId, timestamp);
    }

    private long getTimestampFromMessageRowkey(String rowkey) {
        return Long.parseLong(rowkey.split("_")[1]);
    }

    public void createRoom(Room room) {
        try {
            logger.info("Creating room: " + room);

            Put put = new Put(Bytes.toBytes(getRoomRowkey(room.getId())));
            put.addColumn(Bytes.toBytes(ROOM_INFO_COLUMN_FAMILY), Bytes.toBytes("name"), Bytes.toBytes(room.getName()));
            put.addColumn(Bytes.toBytes(ROOM_INFO_COLUMN_FAMILY), Bytes.toBytes("created"), Bytes.toBytes(room.getCreated()));
            put.addColumn(Bytes.toBytes(ROOM_INFO_COLUMN_FAMILY), Bytes.toBytes("participants"), Bytes.toBytes(String.join(",", room.getParticipants())));
            put.addColumn(Bytes.toBytes(ROOM_META_COLUMN_FAMILY), Bytes.toBytes("count"), Bytes.toBytes(0));
            put.addColumn(Bytes.toBytes(ROOM_META_COLUMN_FAMILY), Bytes.toBytes("created"), Bytes.toBytes(room.getCreated()));
            put.addColumn(Bytes.toBytes(ROOM_META_COLUMN_FAMILY), Bytes.toBytes("lastMsgTs"), Bytes.toBytes(room.getCreated()));
            hbase.putRow(roomTableName, put);
        } catch (IOException e) {
            logger.error("Failed to create room in HBase", e);
            throw new RuntimeException(e);
        }
    }

    public void addMessages(int roomId, List<Message> messages){
        try {
            logger.info("Adding messages to room: " + roomId);

            List<Put> puts = new LinkedList<>();
            for (Message message : messages) {
                Put put = new Put(Bytes.toBytes(getMessageRowkey(roomId, message)));
                put.addColumn(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("index"), Bytes.toBytes(message.getIndex()));
                put.addColumn(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("author"), Bytes.toBytes(message.getAuthor()));
                put.addColumn(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("timestamp"), Bytes.toBytes(message.getTimestamp()));
                put.addColumn(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("message"), Bytes.toBytes(message.getMessage()));
                puts.add(put);
            }
            hbase.putRows(messageTableName, puts);

            Message lastMessage = messages.get(messages.size() - 1);
            updateRoomMetaCountAndLastMessageTs(roomId, lastMessage.getIndex(), lastMessage.getTimestamp());

        } catch (IOException e) {
            logger.error("Failed to add messages to HBase", e);
            throw new RuntimeException(e);
        }
    }

    public List<Message> getMessages(int roomId, long startTime, long endTime) {
        try {
            List<Result> results = hbase.getRowsBetween(messageTableName, MESSAGE_MESSAGE_COLUMN_FAMILY, getMessageRowkey(roomId, startTime), getMessageRowkey(roomId, endTime));
            List<Message> messages = new LinkedList<>();
            for (Result result : results) {
                String author = Bytes.toString(result.getValue(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("author")));
                long timestamp = Bytes.toLong(result.getValue(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("timestamp")));
                int index = Bytes.toInt(result.getValue(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("index")));
                String message = Bytes.toString(result.getValue(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("message")));
                messages.add(new Message(index, timestamp, author, message));
            }

            return messages;
        } catch (IOException e) {
            logger.error("Failed to get messages from HBase", e);
            throw new RuntimeException(e);
        }
    }

    public Room getRoom(int id) {
        try {
            Result result = hbase.getRow(roomTableName, getRoomRowkey(id), ROOM_INFO_COLUMN_FAMILY);
            if (result.isEmpty()) {
                return null;
            } else {
                String name = Bytes.toString(result.getValue(Bytes.toBytes(ROOM_INFO_COLUMN_FAMILY), Bytes.toBytes("name")));
                long created = Bytes.toLong(result.getValue(Bytes.toBytes(ROOM_INFO_COLUMN_FAMILY), Bytes.toBytes("created")));
                List<String> participants = Arrays.asList(Bytes.toString(result.getValue(Bytes.toBytes(ROOM_INFO_COLUMN_FAMILY), Bytes.toBytes("participants"))).split(","));
                RoomMeta meta = getRoomMeta(id);
                Room room = new Room(id, name, created, participants);
                room.setMeta(meta);

                return room;
            }
        } catch (IOException e) {
            logger.error("Failed to get room from HBase", e);
            throw new RuntimeException(e);
        }
    }

    public int countLongPauses(int chatRoomId, long startTime, long endTime) {
        RoomMeta roomMeta = getRoomMeta(chatRoomId);
        if (roomMeta == null) {
            throw new RuntimeException(String.format("Could not find room meta for room id %d", chatRoomId));
        }

        List<Long> pauses = getMessagePauses(chatRoomId, startTime, endTime);
        int longPausesCount = 0;
        if (pauses.isEmpty()) {
            longPausesCount = -1;
        } else {
            long averagePause = (roomMeta.getLastMessageTimestamp() - roomMeta.getCreated()) / roomMeta.getMessageCount();
            logger.info(String.format("Got pauses: %s, average pause: %d", pauses.toString(), averagePause));
            for (long pause : pauses) {
                if (pause > averagePause)
                    longPausesCount++;
            }
        }
        return longPausesCount;
    };

    private RoomMeta getRoomMeta(int id) {
        try {
            Result result = hbase.getRow(roomTableName, getRoomRowkey(id), ROOM_META_COLUMN_FAMILY);
            if (result.isEmpty()) {
                return null;
            } else {
                int count = Bytes.toInt(result.getValue(Bytes.toBytes(ROOM_META_COLUMN_FAMILY), Bytes.toBytes("count")));
                long created = Bytes.toLong(result.getValue(Bytes.toBytes(ROOM_META_COLUMN_FAMILY), Bytes.toBytes("created")));
                long lastMsgTs = Bytes.toLong(result.getValue(Bytes.toBytes(ROOM_META_COLUMN_FAMILY), Bytes.toBytes("lastMsgTs")));

                return new RoomMeta(count, created, lastMsgTs);
            }
        } catch (IOException e) {
            logger.error("Failed to get room meta from HBase", e);
            throw new RuntimeException(e);
        }
    }

    private List<Long> getMessagePauses(int roomId, long startTime, long endTime) {
        try {
            List<Result> results = hbase.getRowsBetween(messageTableName, MESSAGE_MESSAGE_COLUMN_FAMILY, getMessageRowkey(roomId, startTime), getMessageRowkey(roomId, endTime));
            List<Long> msgTimestamps = new LinkedList<>();
            for (Result result : results) {
                long ts = Bytes.toLong(result.getValue(Bytes.toBytes(MESSAGE_MESSAGE_COLUMN_FAMILY), Bytes.toBytes("timestamp")));
                msgTimestamps.add(ts);
            }

            List<Long> pauses = new LinkedList<>();
            if (!msgTimestamps.isEmpty()) {
                long lastTimestamp = msgTimestamps.get(0);
                for (long ts: msgTimestamps) {
                    pauses.add(ts - lastTimestamp);
                    lastTimestamp = ts;
                }
            }

            return pauses;
        } catch (IOException e) {
            logger.error("Failed to get message pauses from HBase", e);
            throw new RuntimeException(e);
        }
    }

    private void updateRoomMetaCountAndLastMessageTs(int roomId, int index, long messageTimestamp){
        try {
            hbase.checkAndPut(roomTableName, getRoomRowkey(roomId), ROOM_META_COLUMN_FAMILY, "count", CompareFilter.CompareOp.GREATER, Bytes.toBytes(index));
            hbase.checkAndPut(roomTableName, getRoomRowkey(roomId), ROOM_META_COLUMN_FAMILY, "lastMsgTs", CompareFilter.CompareOp.GREATER, Bytes.toBytes(messageTimestamp));
        } catch (IOException e) {
            logger.error("Failed to update room meta data");
            throw new RuntimeException(e);
        }
    }

}
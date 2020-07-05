package org.wayneyu.chat.db;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wayneyu.chat.hbase.HBaseService;
import org.wayneyu.chat.model.Message;
import org.wayneyu.chat.model.Room;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

public class HbaseRepositoryImplTest {

    private static final String roomTableName = "roomTest";
    private static final String messageTableName = "messageTest";
    private ChatRepository repo = new HBaseRepositoryImpl(roomTableName, messageTableName);
    private static HBaseService hbase = new HBaseService();

    private static final Room room = new Room(1, "testRoom", 0, Arrays.asList("a@a.com", "b@b.com"));

    @BeforeClass
    public static void init() {
        hbase.init();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        hbase.deleteTable(roomTableName);
        hbase.deleteTable(messageTableName);
    }

    @Test
    public void shouldCreateRoom() {
        repo.createRoom(room);

        assertEquals(room, repo.getRoom(1));
    }

    @Test
    public void shouldAddMessages() {
        int roomId = 2;
        repo.createRoom(new Room(roomId, "testRoom2", 0, Arrays.asList("a@a.com", "b@b.com")));

        Message msg1 = new Message(1, 1, "a@a.com", "helloA");
        Message msg2 = new Message(2, 2, "b@b.com", "helloB");
        repo.addMessages(roomId, Arrays.asList(msg1, msg2));

        List<Message> actual = repo.getMessages(roomId, 0, 3);

        assertThat(actual, contains(msg1, msg2));
    }

    @Test
    public void shouldNotReturnDuplicateMessages() {
        int roomId = 4;
        repo.createRoom(new Room(roomId, "testRoom4", 0, Arrays.asList("a@a.com", "b@b.com")));

        Message msg1 = new Message(1, 1, "a@a.com", "helloA");
        Message msg2 = new Message(2, 2, "a@a.com", "helloA");
        Message msg3 = new Message(3, 3, "b@b.com", "helloB");
        repo.addMessages(roomId, Arrays.asList(msg1, msg2));
        repo.addMessages(roomId, Arrays.asList(msg2, msg3));

        List<Message> actual = repo.getMessages(roomId, 0, 4);

        assertThat(actual, contains(msg1, msg2, msg3));
    }

    @Test
    public void shouldGetLongPause() {
        int roomId = Integer.MAX_VALUE;
        repo.createRoom(new Room(roomId, "testRoom5", 1578283920000L, Arrays.asList("a@a.com", "b@b.com")));

        Message msg1 = new Message(1, 1578283920001L, "b@b.com", "helloB2");
        Message msg2 = new Message(2, 1578283920002L, "b@b.com", "helloB2");
        Message msg3 = new Message(3, 1578283920003L, "b@b.com", "helloB2");
        Message msg4 = new Message(4, 1578283920010L, "b@b.com", "helloB3");
        Message msg5 = new Message(5, 1578283920012L, "b@b.com", "helloB3");
        repo.addMessages(roomId, Arrays.asList(msg1, msg2, msg3, msg4, msg5));
        int actual = repo.countLongPauses(roomId, 1578283920000L,  1578283920011L);

        assertEquals(1, actual);
    }

    @Test
    public void shouldGetLongPauseIfMessagesWereResent() {
        int roomId = 6;
        repo.createRoom(new Room(roomId, "testRoom6", 0, Arrays.asList("a@a.com", "b@b.com")));

        Message msg1 = new Message(1, 0, "a@a.com", "helloA");
        Message msg2 = new Message(2, 10, "b@b.com", "helloB1");
        Message msg3 = new Message(3, 20, "b@b.com", "helloB2");
        Message msg4 = new Message(4, 100, "b@b.com", "helloB3");

        repo.addMessages(roomId, Arrays.asList(msg1, msg2, msg3, msg4));
        repo.addMessages(roomId, Arrays.asList(msg2, msg3));

        int actual = repo.countLongPauses(roomId, 0,  101);

        assertEquals(actual, 1);
    }

}

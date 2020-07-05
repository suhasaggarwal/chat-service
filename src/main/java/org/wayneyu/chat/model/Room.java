package org.wayneyu.chat.model;

import java.util.List;
import java.util.Objects;

public class Room {

    private int id;
    private String name;
    private long created;
    private List<String> participants;
    private RoomMeta meta;

    public Room(int id, String name, long created, List<String> participants) {
        this.id = id;
        this.name = name;
        this.created = created;
        this.participants = participants;
    }

    public int getId(){return id;}
    public String getName(){return name;}
    public long getCreated(){return created;}
    public List<String> getParticipants(){return participants;}

    public void setMeta(RoomMeta meta) {
        this.meta = meta;
    }

    @Override
    public String toString() {
        return "Room{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", created=" + created +
                ", participants=" + participants +
                ", meta=" + meta +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Room room = (Room) o;
        return id == room.id &&
                created == room.created &&
                name.equals(room.name) &&
                participants.equals(room.participants);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, created, participants);
    }
}
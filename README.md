# Chat-service
Simple RESTful chat service API with HBase as persistence layer

### Setup
Download HBase 1.2.6 [link](https://archive.apache.org/dist/hbase/1.2.6/)

Extract and cd to install folder

Edit `./conf/hbase-site.xml`
```
<property>
    <name>hbase.rootdir</name>
    <value>/path/to/hbase</value>
</property>
<property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/path/to/zookeeper</value>
</property>
<property>
      <name>hbase.zookeeper.property.clientPort</name>
      <value>2181</value>
  </property>
</configuration>
```

Start HBase standalone
```
./bin/start-hbase.sh
```

### Running the REST API service
Run App.java, service available at
```
localhost:9999
```

 
### REST API

Create chat room
```
PUT /room
{
    "id": 1234,
    "name": "PublicChatRoom1",
    "created": 1578281201000,
    "participants": [
        "john.doe@mail.com",
        "jessica.doe@mail.com",
        "colin.powel@mail.com"
    ]
}'
```

Add new messages
```
PUT /messages
{
    "chatRoomId": 1234,
    "messages": [
        {
            "index": 1,
            "timestamp": 1578283201000,
            "author": "john.doe@mail.com",
            "message": "hi, what's up?"
        },
        {
            "index": 2,
            "timestamp": 1578283800000,
            "author": "jessica.doe@mail.com",
            "message": "not much"
        },
        {
            "index": 3,
            "timestamp": 1578283920000,
            "author": "colin.powel@mail.com",
            "message": "what are you up to guys?"
        },
    ]
}
```

Get room
```
GET /room/:id
```

Get messages from room between start <= t < end
```
GET /room/:id/messages/start/end
``` 

Count long pauses for messages from room between start <= t < end
```
GET /room/:id/long-pauses/start/end
```

### HBase table and rowkey design
Access pattern
1. request chat entries from a specific chat room by time range
2. count long pauses between chat entries inside a time range (a long pause is
any pause longer than the average time between entries in a given chat room)

Chat room schema
```
row             column
chatRoomId      info:created
                info:name
                info:participants
                meta:created     // same as info:created
                meta:count       // nunber of messages in room
                meta:lastMsgTs   // timestamp of last message
```

Messages schema
```
row                     column
chatRoomId_timestamp    message.index
                        message.author
                        message.timestamp
                        message.message
```
chatRoomId and timestamp are zero padded to 10 and 13 digits to allow correct alphabetical sorting

With this design, access pattern above are accomplished by:
1. Range scan Messages table with rowkey between chatRoomId_start and chatRoomId_end 
2. Range scan Messages table to get timestamp of messages, get Room meta data, return messages where (timestamp - last_message_timestamp) > (meta:lastMsgTs - meta:created)/meta:count


### Unresolved edge cases
As HBase lookup is by rowkey or range of rowkeys, looking up a row preceeding another row is not straightforward. Thus, when we calculate pauses between messages, we default pause of first returned message to 0.

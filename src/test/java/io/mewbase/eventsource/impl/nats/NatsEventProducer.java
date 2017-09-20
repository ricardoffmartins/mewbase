package io.mewbase.eventsource.impl.nats;


import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.TestEventProducer;
import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import java.util.UUID;
import java.util.stream.LongStream;


public class NatsEventProducer implements TestEventProducer {

    final String userName = "TestClient";
    final String clusterName = "test-cluster";
    final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
    final Connection nats;
    final String channelName;

    public NatsEventProducer() throws Exception {
        this("Channel1");   // default channel name
    }

    public NatsEventProducer(String channelName) throws Exception {
        this.channelName = channelName;
        cf.setNatsUrl("nats://localhost:4222");
        cf.setClientId(UUID.randomUUID().toString());
        nats = cf.createConnection();
    }


    public void sendEvent(BsonObject  event) throws Exception {
        nats.publish( channelName, event.encode().getBytes() );
    }

    public void sendNumberedEvents(Long startInclusive, Long endInclusive) throws Exception {
        LongStream.rangeClosed(startInclusive,endInclusive).forEach( l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            try {
                sendEvent(bsonEvent);
            } catch(Exception e) {
                // wrap and rethrow
                throw new RuntimeException(e);
            }
        } );
    }


    public void close() throws Exception {
        nats.close();
    }

}

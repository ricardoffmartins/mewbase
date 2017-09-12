package io.mewbase.eventsource.impl.nats;


import io.mewbase.bson.BsonObject;
import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import java.util.UUID;


public class NatsEventProducer implements io.mewbase.eventsource.TestEventProducer {

    // TODO - Parameterise from server params
    final String userName = "TestClient";
    final String clusterName = "test-cluster";
    final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
    final Connection nats;
    final String channelName;


    public NatsEventProducer() throws Exception {
        this("Channel1");
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

    public void sendNumberedEvents(Long startNumber, Long endNumber) throws Exception {
        throw new UnsupportedOperationException("sendNumberedEvents not implemented");
    }

}

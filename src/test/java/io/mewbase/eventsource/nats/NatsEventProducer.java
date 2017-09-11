package io.mewbase.eventsource.nats;

import io.mewbase.bson.Bson;
import io.mewbase.bson.BsonObject;
import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import java.io.IOException;

public class NatsEventProducer implements io.mewbase.eventsource.TestEventProducer {

    // TODO - Parameterise from server params
    final String userName = "TestClient";
    final String clusterName = "test-cluster";
    final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
    final Connection nats;
    final String channelName;


    public NatsEventProducer() throws Exception {
        this("DefaultTestChannel");
    }

    public NatsEventProducer(String channelName) throws Exception {
        this.channelName = channelName;
        cf.setNatsUrl("nats://localhost:4222");
        nats = cf.createConnection();
    }


    void sendEvent(BsonObject  event) throws IOException {
        nats.publish( channelName, event.encode().getBytes() );
    }

    void sendNumberedEvents(Long startNumber, Long endNumber) throws Exception {
        throw new UnsupportedOperationException("sendNumberedEvents not implemented");1
    }

}

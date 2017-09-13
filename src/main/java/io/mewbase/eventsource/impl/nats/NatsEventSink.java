package io.mewbase.eventsource.impl.nats;



import io.mewbase.eventsource.EventSink;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


/**
 * These tests assume that there is an instance of Nats Streaming Server running on localhost:4222
 */

public class NatsEventSink implements EventSink {

    private final static Logger logger = LoggerFactory.getLogger(NatsEventSink.class);

    final String userName = "TestClient";
    final String clusterName = "test-cluster";
    final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
    Connection nats = null;

    // TODO - Handle params and defaults

    public NatsEventSink() {
        cf.setNatsUrl("nats://localhost:4222");
        try {
            cf.setClientId(UUID.randomUUID().toString());
            nats = cf.createConnection();
        } catch (Exception exp) {
            logger.error("Error connecting to Nats Streaming Server", exp);
            throw new RuntimeException(exp);
        }
    }


    @Override
    public void publish(String channelName, byte [] bytes) {
        try {
            nats.publish(channelName, bytes);
        } catch (Exception exp) {
            logger.error("Error attempting publish event to Nats Event Sink", exp);
        }
    }


    @Override
    public void close() {
        try {
            nats.close();
        } catch (Exception exp) {
            logger.error("Error attempting close Nats Streaming Server Event Sink", exp);
        }
    }

}

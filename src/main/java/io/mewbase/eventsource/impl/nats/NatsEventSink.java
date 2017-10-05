package io.mewbase.eventsource.impl.nats;


import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import io.mewbase.server.MewbaseOptions;
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


    private final Connection nats;


    public NatsEventSink() {
        this(new MewbaseOptions());
    }


    public NatsEventSink(MewbaseOptions mewbaseOptions) {
        final String userName = mewbaseOptions.getSinkUserName();;
        final String clusterName = mewbaseOptions.getSinkClusterName();
        final String url = mewbaseOptions.getSinkUrl();

        final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
        cf.setNatsUrl(url);

        try {
            String clientUUID = UUID.randomUUID().toString();
            cf.setClientId(clientUUID);
            nats = cf.createConnection();
            logger.info("Created Nats EventSink connection with client UUID " + clientUUID);
        } catch (Exception exp) {
            logger.error("Error connecting to Nats Streaming Server", exp);
            throw new RuntimeException(exp);
        }
    }


    @Override
    public void publish(String channelName, BsonObject event) {
        try {
            nats.publish(channelName, event.encode().getBytes());
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

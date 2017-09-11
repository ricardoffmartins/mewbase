package io.mewbase.eventsource.impl.nats;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;

import io.nats.stan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These tests assume that there is an instance of Nats Streaming Server running on localhost:4222
 */

public class NatsEventSource implements EventSource {

    private final static Logger logger = LoggerFactory.getLogger(NatsEventSource.class);

    final String userName = "TestClient";
    final String clusterName = "test-cluster";
    final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
    Connection nats = null;

    // TODO - Handle params and defaults

    public NatsEventSource() {
        cf.setNatsUrl("nats://localhost:4222");
        try {
            nats = cf.createConnection();
        } catch (Exception exp) {
            logger.error("Error connecting to Nats Streaming Server", exp);
            throw new RuntimeException(exp);
        }
    }

    @Override
    public Subscription subscribe(String channelName, EventHandler eventHandler) {

        MessageHandler handler = new MessageHandler() {
            public void onMessage(Message m) {
                Event evt = new NatsEvent(m);
                eventHandler.onEvent(evt);
            }
        };
        SubscriptionOptions opts = new SubscriptionOptions.Builder().deliverAllAvailable().build();
        Subscription subs = null;
        try {
            subs = new NatsSubscription( nats.subscribe(channelName, handler, opts) );
        } catch (Exception exp) {
           logger.error("Error attempting to subscribe to Nats Streaming Server", exp);
        }
        return subs;
    }



}

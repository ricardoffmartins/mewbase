package io.mewbase.eventsource.impl.nats;


import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;

import io.mewbase.server.MewbaseOptions;
import io.nats.stan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;


/**
 * An Event Source implemented by the Nats Streaming Server.
 */

public class NatsEventSource implements EventSource {

    private final static Logger logger = LoggerFactory.getLogger(NatsEventSource.class);


    private final Connection nats;


    public NatsEventSource() {
        this(new MewbaseOptions());
    }

    public NatsEventSource(MewbaseOptions mewbaseOptions) {

        final String userName = mewbaseOptions.getSourceUserName();
        final String clusterName = mewbaseOptions.getSourceClusterName();
        final String url = mewbaseOptions.getSourceUrl();

        final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
        cf.setNatsUrl(url);

        try {
            cf.setClientId(UUID.randomUUID().toString());
            nats = cf.createConnection();
        } catch (Exception exp) {
            logger.error("Error connecting to Nats Streaming Server", exp);
            throw new RuntimeException(exp);
        }
    }


    private Subscription subscribeWithOptions(String channelName, EventHandler eventHandler, SubscriptionOptions opts) {
        MessageHandler handler = message -> {
            eventHandler.onEvent(new NatsEvent(message));
        };
        Subscription subs = null;
        try {
            subs = new NatsSubscription( nats.subscribe(channelName, handler, opts) );
        } catch (Exception exp) {
            logger.error("Error attempting to subscribe to Nats Streaming Server", exp);
        }
        return subs;
    }


    @Override
    public Subscription subscribe(String channelName, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        return subscribeWithOptions( channelName, eventHandler, opts);
    }

    @Override
    public Subscription subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startWithLastReceived().build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }

    @Override
    public Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startAtSequence(startInclusive).build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }

    @Override
    public Subscription subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startAtTime(startInstant).build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }

    @Override
    public Subscription subscribeAll(String channelName, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().deliverAllAvailable().build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }


    @Override
    public void close() {
        try {
            nats.close();
        } catch (Exception exp) {
            logger.error("Error attempting close Nats Streaming Server Event source", exp);
        }
    }

}

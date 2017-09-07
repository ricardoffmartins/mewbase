package io.mewbase.eventsource.impl.nats;

import io.nats.stan.Subscription;


public class NatsSubscription implements io.mewbase.eventsource.Subscription {

    final Subscription subs;

    NatsSubscription(Subscription subs) {
        this.subs = subs;
    }

    @Override
    public void unsubscribe()   {
        try {
            subs.unsubscribe();
        } catch (Exception e) {
            // TODO log it
        }
    }

    @Override
    public void close() {
        subs.close();
    }

    @Override
    public String getChannelName() {
        return subs.getQueue();
    }



}

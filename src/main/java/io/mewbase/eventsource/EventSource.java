package io.mewbase.eventsource;

public interface EventSource {


    Subscription subscribe(String channelName, EventHandler eventHandler);

    // TODO
    // Subscribe from Event no
    // Subscribe fromm Instant
    // Subscribe all
    // and possibly more

    void close();

}

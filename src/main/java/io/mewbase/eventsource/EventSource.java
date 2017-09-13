package io.mewbase.eventsource;

public interface EventSource {

    /**
     * Subscribe to a named channel with the given event handler.
     * Any new Events that arrive at the source will be sent to the event handler.
     * @param channelName
     * @param eventHandler
     * @return
     */
    Subscription subscribe(String channelName, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler.
     * Replay the events from the given number
     * @param channelName
     * @param eventHandler
     * @return
     */
    Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler);



    // Subscribe from Instant
    // Subscribe all

    void close();

}

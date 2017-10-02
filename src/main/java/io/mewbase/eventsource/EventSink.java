package io.mewbase.eventsource;


import io.mewbase.bson.BsonObject;

public interface EventSink {

    /**
     * Publish an Event in the form of a byte array to a named channel.
     * Any new Events that arrive at the source will be sent to the event handler.
     * @param channelName
     * @param event as a byte array.
     */
    // TODO Make a CompletableFuture with an ack handler in the impl
    void publish(String channelName, BsonObject event);

    /**
     * Close down this EventSink and all its associated resources
     */
    void close();

}

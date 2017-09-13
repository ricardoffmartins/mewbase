package io.mewbase.eventsource;



public interface EventSink {

    /**
     * Publish an Event in the form of a byte array to a named channel.
     * Any new Events that arrive at the source will be sent to the event handler.
     * @param channelName
     * @param event as a byte array.
     */
    void publish(String channelName, byte [] event);

    /**
     * Close down this EventSink and all its associated resources
     */
    void close();

}

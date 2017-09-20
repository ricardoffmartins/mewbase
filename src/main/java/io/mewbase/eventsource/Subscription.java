package io.mewbase.eventsource;

public interface Subscription {

    void unsubscribe();

    void close();

}

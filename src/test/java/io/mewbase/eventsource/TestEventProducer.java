package io.mewbase.eventsource;

import io.mewbase.bson.BsonObject;

import java.io.IOException;


public interface TestEventProducer {

    void sendEvent(BsonObject event) throws IOException;

    void sendNumberedEvents(Long startNumber, Long endNumber) throws IOException;

}

package io.mewbase.eventsource;

import io.mewbase.bson.BsonObject;

import java.io.IOException;


public interface TestEventProducer {

    void sendEvent(BsonObject event) throws Exception;

    void sendNumberedEvents(Long startNumber, Long endNumber) throws Exception;

}

package io.mewbase.eventsource;

import io.mewbase.bson.BsonObject;
import io.vertx.core.buffer.Buffer;

import java.time.Instant;


public interface Event {

    BsonObject getBson();

    Instant getInstant();

    Long getEventNumber();

    int getCrc32();
}

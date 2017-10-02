package io.mewbase.eventsource;

import io.mewbase.bson.BsonObject;
import io.vertx.core.buffer.Buffer;

import java.time.Instant;


public interface Event {

    default BsonObject getBson() {
        return new BsonObject(Buffer.buffer(getData()));
    }

    byte[] getData();

    Instant getInstant();

    Long getEventNumber();

    int getCrc32();
}

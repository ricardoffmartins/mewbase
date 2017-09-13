package io.mewbase.eventsource;

import java.time.Instant;


public interface Event {

    byte[] getData() ;

    Instant getInstant();

    Long getEventNumber();

    int getCrc32();
}

package io.mewbase.eventsource.impl.nats;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.nats.stan.Message;
import io.vertx.core.buffer.Buffer;

import java.time.Instant;


class NatsEvent implements Event {

    final Message msg;

    NatsEvent(Message msg) {
        this.msg = msg;
    }

    @Override
    public BsonObject getBson() {
            return new BsonObject(Buffer.buffer(msg.getData()));
    }

    @Override
    public Instant getInstant()  { return msg.getInstant(); }

    @Override
    public Long getEventNumber() { return msg.getSequence(); }

    @Override
    public int getCrc32() { return msg.getCrc32(); }

    // TODO - Possibly add acknowledge (ack) to this - does it leak impl - discuss ???

}

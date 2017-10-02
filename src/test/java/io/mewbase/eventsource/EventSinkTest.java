package io.mewbase.eventsource;

import io.mewbase.ServerTestBase;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.TestEventProducer;
import io.mewbase.eventsource.impl.nats.NatsEventProducer;
import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

/**
 * Created by Nige on 7/9/2017.
 */
@RunWith(VertxUnitRunner.class)
public class EventSinkTest extends ServerTestBase {


    @Test
    public void testConnectToEventSink() throws Exception {
        EventSink es = new NatsEventSink();
        es.close();
        assert(true);
    }


    @Test
    public void testPublishSingleEvent() throws Exception {

        EventSink eSink = new NatsEventSink();

        final String testChannelName = "singleEventSink";
        final String inputUUID = randomString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        // check the event arrived
        final CountDownLatch latch = new CountDownLatch(1);
        EventSource eSource = new NatsEventSource();
        Subscription subs = eSource.subscribe(testChannelName,  event ->  {
                        BsonObject bson  = event.getBson();
                        assert(inputUUID.equals(bson.getString("data")));
                        latch.countDown();
                        }
                    );

        eSink.publish(testChannelName,bsonEvent);

        latch.await();

        eSource.close();
        eSink.close();
    }


    @Test
    public void testManyEventsInOrder() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestMultiEventChannel";
        EventSink eSink = new NatsEventSink();

        final int START_EVENT_NUMBER = 1;
        final int END_EVENT_NUMBER = 512;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        EventSource eSource = new NatsEventSource();
        eSource.subscribe(testChannelName, event -> {
                BsonObject bson  = event.getBson();
                long thisEventNum = END_EVENT_NUMBER - latch.getCount();
                assert(bson.getLong("num") == thisEventNum);
                latch.countDown();
        });


        LongStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).forEach(l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            eSink.publish(testChannelName,bsonEvent);
        } );


        latch.await();
        eSource.close();
        eSink.close();
    }


 }
package io.mewbase.eventsource;

import io.mewbase.MewbaseTestBase;
import io.mewbase.ServerTestBase;

import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.mewbase.eventsource.impl.nats.NatsEventProducer;

import io.mewbase.server.MewbaseOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * Created by Nige on 7/9/2017.
 */
@RunWith(VertxUnitRunner.class)
public class EventSourceTest extends MewbaseTestBase {


    @Test
    public void testConnectToEventSource() throws Exception {
        EventSource es = new NatsEventSource(new MewbaseOptions());
        es.close();
        assert (true);
    }


    @Test
    public void testSingleEvent() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestSingleEventChannel";
        TestEventProducer prod = new NatsEventProducer(testChannelName);

        final String inputUUID = randomString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        final CountDownLatch latch = new CountDownLatch(1);
        EventSource es = new NatsEventSource(new MewbaseOptions());
        Subscription subs = es.subscribe(testChannelName,  event ->  {
                        BsonObject bson  = event.getBson();
                        assert(inputUUID.equals(bson.getString("data")));
                        long evtNum = event.getEventNumber();
                        Instant evtTime = event.getInstant();
                        int evtHashc = event.getCrc32();
                        latch.countDown();
                        }
                    );

        prod.sendEvent(bsonEvent);

        latch.await();

        subs.unsubscribe();

        es.close();

        prod.close();
    }


    @Test
    public void testManyEventsInOrder() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestMultiEventChannel";
        TestEventProducer prod = new NatsEventProducer(testChannelName);

        final int START_EVENT_NUMBER = 1;
        final int END_EVENT_NUMBER = 512;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        EventSource es = new NatsEventSource();
        es.subscribe(testChannelName, event -> {
                BsonObject bson =  event.getBson();
                long thisEventNum = END_EVENT_NUMBER - latch.getCount();
                assert(bson.getLong("num") == thisEventNum);
                latch.countDown();
        });

        prod.sendNumberedEvents((long)START_EVENT_NUMBER, (long)END_EVENT_NUMBER);

        latch.await();
        es.close();

        prod.close();
    }


    @Test
    public void testMostRecent() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestMostRecentChannel";
        TestEventProducer prod = new NatsEventProducer(testChannelName);

        final int START_EVENT_NUMBER = 1;
        final long MID_EVENT_NUMBER = 256;
        final int END_EVENT_NUMBER = 512;

        final int eventsToTest = 100;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);

        prod.sendNumberedEvents((long)START_EVENT_NUMBER, (long)MID_EVENT_NUMBER);

        EventSource es = new NatsEventSource(new MewbaseOptions());
        es.subscribeFromMostRecent(testChannelName, event -> {
            BsonObject bson = event.getBson();
            long thisEventNum = MID_EVENT_NUMBER + (eventsToTest - latch.getCount());
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        prod.sendNumberedEvents((long)MID_EVENT_NUMBER+1, (long)END_EVENT_NUMBER);

        latch.await();
        es.close();

        prod.close();
    }


    @Test
    public void testSubscribeFromEventNumber() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestFromNumberChannel";
        TestEventProducer prod = new NatsEventProducer(testChannelName);

        final int START_EVENT_NUMBER = 1;
        final long MID_EVENT_NUMBER = 256;
        final int END_EVENT_NUMBER = 512;

        final int eventsToTest = 100;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);

        prod.sendNumberedEvents((long)START_EVENT_NUMBER, (long)END_EVENT_NUMBER);

        EventSource es = new NatsEventSource();
        es.subscribeFromEventNumber(testChannelName, MID_EVENT_NUMBER, event -> {
            BsonObject bson = event.getBson();
            long thisEventNum = MID_EVENT_NUMBER + (eventsToTest - latch.getCount());
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        latch.await();
        es.close();

        prod.close();
    }

    @Test
    public void testSubscribeFromInstant() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestFromInstantChannel";
        TestEventProducer prod = new NatsEventProducer(testChannelName);

        final int START_EVENT_NUMBER = 1;
        final long MID_EVENT_NUMBER = 256;
        final int END_EVENT_NUMBER = 512;

        final int eventsToTest = 100;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);


        prod.sendNumberedEvents((long)START_EVENT_NUMBER, MID_EVENT_NUMBER);

        Thread.sleep(100); // give the events time to rest in the event source

        Instant then = Instant.now();

        Thread.sleep(100); // some room the other side of the time window

        prod.sendNumberedEvents((long)MID_EVENT_NUMBER+1, (long)END_EVENT_NUMBER);

        EventSource es = new NatsEventSource();
        es.subscribeFromInstant(testChannelName, then, event -> {
            BsonObject bson  = event.getBson();
            long thisEventNum = MID_EVENT_NUMBER + 1 + (eventsToTest - latch.getCount());
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        latch.await();
        es.close();

        prod.close();
    }


    @Test
    public void testSubscribeAll() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestAllChannel";
        TestEventProducer prod = new NatsEventProducer(testChannelName);

        final int START_EVENT_NUMBER = 1;
        final long MID_EVENT_NUMBER = 256;
        final int END_EVENT_NUMBER = 512;

        final int eventsToTest = END_EVENT_NUMBER;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);

        prod.sendNumberedEvents((long)START_EVENT_NUMBER, MID_EVENT_NUMBER);

        EventSource es = new NatsEventSource();
        es.subscribeAll(testChannelName,  event -> {
            BsonObject bson = event.getBson();
            long thisEventNum = START_EVENT_NUMBER + eventsToTest - latch.getCount();
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        prod.sendNumberedEvents( MID_EVENT_NUMBER+1, (long)END_EVENT_NUMBER );

        latch.await();
        es.close();

        prod.close();
    }


}
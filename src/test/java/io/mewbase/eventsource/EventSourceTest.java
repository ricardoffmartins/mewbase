package io.mewbase.eventsource;

import io.mewbase.ServerTestBase;

import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.mewbase.eventsource.impl.nats.NatsEventProducer;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

/**
 * Created by Nige on 7/9/2017.
 */
@RunWith(VertxUnitRunner.class)
public class EventSourceTest extends ServerTestBase {

//    @Override
//    protected void setup(TestContext context) throws Exception {
//        super.setup(context);
//
//    }

//    TestEventProducer prod = new NatsEventProducer(DEFAULT_CHANNEL);
//
//
//    @Before
//    public void setupTestEventProducer() throws Exception {
//       prod = new NatsEventProducer(DEFAULT_CHANNEL);
//    }
//
//    @After
//    public void closeTestEventProducer() throws Exception {
//       prod.close();
//    }


    @Test
    public void testConnectToEventSource() throws Exception {
        EventSource es = new NatsEventSource();
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
        EventSource es = new NatsEventSource();
        es.subscribe(testChannelName,  event ->  {
                        BsonObject bson  = new BsonObject(Buffer.buffer(event.getData()));
                        assert(inputUUID.equals(bson.getString("data")));
                        latch.countDown();
                        }
                    );

        prod.sendEvent(bsonEvent);

        latch.await();
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
                BsonObject bson  = new BsonObject(Buffer.buffer(event.getData()));
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
            BsonObject bson  = new BsonObject(Buffer.buffer(event.getData()));
            long thisEventNum = MID_EVENT_NUMBER + (eventsToTest - latch.getCount());
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        latch.await();
        es.close();

        prod.close();
    }


}
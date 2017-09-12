package io.mewbase.eventsource;

import io.mewbase.ServerTestBase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.mewbase.eventsource.impl.nats.NatsEventProducer;
import io.nats.stan.Message;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
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
    public final String DEFAULT_CHANNEL = "Channel1";
    public final TestEventProducer prod = new NatsEventProducer(DEFAULT_CHANNEL);

    public EventSourceTest() throws Exception {
    }

    @Test
    public void testConnectToEventSource() throws Exception {
        EventSource es = new NatsEventSource();
        es.close();
        assert (true);
    }


    @Test
    public void testEventSourceSingleEvent() throws Exception {

        final String inputUUID = randomString();
        final BsonObject bson = new BsonObject().put("data",inputUUID);

        final CountDownLatch latch = new CountDownLatch(2);
        EventSource es = new NatsEventSource();
        es.subscribe(DEFAULT_CHANNEL, new EventHandler() {
                    public void onEvent(Event  evt) {
                        latch.countDown();
                        BsonObject bson  = new BsonObject(Buffer.buffer(evt.getData()));
                        assert(inputUUID.equals(bson.getString("data")));
                        latch.countDown();
            }
        });
        latch.await();
        es.close();
    }



}
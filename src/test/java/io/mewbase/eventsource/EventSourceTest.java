package io.mewbase.eventsource;

import io.mewbase.ServerTestBase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.mewbase.eventsource.nats.NatsEventProducer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

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
    public final TestEventProducer = new NatsEventProducer;

    @Test
    public void testConnect() throws Exception {
        EventSource es = new NatsEventSource();
        // es.subscribe("Channel1", new )]assert
        assert (true);
    }

    @Test
    public void EventSourceSingleMessage throws Exception {
        EventSource es = new NatsEventSource();
        es.subscribe(DEFAULT_CHANNEL, new
                // TODO call back
            // call back
        )


        BsonObject bson = new BsonObject();
        bson

        / check the call back

    }



}
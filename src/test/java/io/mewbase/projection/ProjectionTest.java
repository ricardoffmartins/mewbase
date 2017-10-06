package io.mewbase.projection;

import io.mewbase.MewbaseTestBase;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSource;


import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class ProjectionTest extends MewbaseTestBase {

    private static final String TEST_CHANNEL = "ProjectionTestChannel";
    private static final String TEST_BINDER = "ProjectionTestBinder";
    private static final String TEST_PROJECTION_NAME = "TestProjection";

    private BinderStore store = null;
    private EventSource source = null;

    @Before
    public void before() throws Exception {
        store = new LmdbBinderStore(createMewbaseOptions());
        source = new NatsEventSource();
    }

    @After
    public void after() {
        store.close();
        source.close();
    }


    @Test
    public void testProjectionFactory() throws Exception {
        ProjectionFactory factory = ProjectionFactory.instance(source,store);
        assertNotNull(factory);
        ProjectionBuilder builder = factory.builder();
        assertNotNull(builder);
    }


    @Test
    public void testProjectionBuilder() throws Exception {

        ProjectionFactory factory = ProjectionFactory.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        Projection projection = createProjection(builder, TEST_PROJECTION_NAME);

        assertNotNull(projection);
        assertEquals(TEST_PROJECTION_NAME, projection.getName());

        projection.stop();
    }



    @Test
    // @Repeat(50)
    public void testSimpleProjectionRuns() throws Exception {

        ProjectionFactory factory = ProjectionFactory.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        final String BASKET_ID_FIELD = "BasketID";
        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(27);

        final CountDownLatch latch = new CountDownLatch(1);


        Projection projection = builder
                .named(TEST_PROJECTION_NAME)
                .projecting(TEST_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    BsonObject out = event.getBson().put("output",RESULT);
                    latch.countDown();
                    return out;
                })
                .create();

        // Send an event to the channel which the projection is subscribed to.
        EventSink sink = new NatsEventSink();
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publish(TEST_CHANNEL, evt);

        latch.await();

        // try to recover the new document
        Binder binder = store.open(TEST_BINDER).get();
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(basketDoc);
        assertEquals(RESULT,basketDoc.getInteger("output"));

       projection.stop();
    }


    @Test
    public void testProjectionNames() throws Exception {

        ProjectionFactory factory = ProjectionFactory.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        Stream<String> names = IntStream.range(1,10).mapToObj( i -> {
            final String projName = "Proj" + i;
            createProjection(builder,projName);
            return projName;
        });

        assertTrue( names.allMatch( name -> factory.isProjection(name) ) );
    }

    @Test
    public void testProjectionRecoversFromEventNumber() throws Exception {

        ProjectionFactory factory = ProjectionFactory.instance(source,store);
        ProjectionBuilder builder = factory.builder();

        final String BASKET_ID_FIELD = "BasketID";
        final String TEST_BASKET_ID = "TestBasket";
        final Integer RESULT = new Integer(27);

        final CountDownLatch latch = new CountDownLatch(1);

        final String MULTI_EVENT_CHANNEL = "MultiEventChannel";

        Projection projection = builder
                .named(TEST_PROJECTION_NAME)
                .projecting(MULTI_EVENT_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    BsonObject out = event.getBson().put("output",RESULT);
                    latch.countDown();
                    return out;
                })
                .create();

        // Send an event to the channel which the projection is subscribed to.
        EventSink sink = new NatsEventSink();
        BsonObject evt = new BsonObject().put(BASKET_ID_FIELD, TEST_BASKET_ID);
        sink.publish(MULTI_EVENT_CHANNEL, evt);

        latch.await();

        // Recover the new document
        Binder binder = store.open(TEST_BINDER).get();
        BsonObject basketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(basketDoc);
        assertEquals(RESULT,basketDoc.getInteger("output"));

        projection.stop();

        // binder now has offset event and valid current document
        ProjectionFactory newFactory = ProjectionFactory.instance(source,store);
        ProjectionBuilder newBuilder = newFactory.builder();

        final CountDownLatch newLatch = new CountDownLatch(1);

        // make new projection of the same name
        Projection newProjection = newBuilder
                .named(TEST_PROJECTION_NAME)
                .projecting(MULTI_EVENT_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(BASKET_ID_FIELD))
                .as( (basket, event) -> {
                    final int currentVal = basket.getInteger("output");
                    basket.put("output",RESULT+currentVal);
                    newLatch.countDown();
                    return basket;
                })
                .create();

        // send another event on the same channel
        sink.publish(MULTI_EVENT_CHANNEL, evt);
        // and wait for the result
        newLatch.await();

        // Recover the new document
        BsonObject newBasketDoc = binder.get(TEST_BASKET_ID).get();
        assertNotNull(newBasketDoc);
        assertEquals(RESULT+RESULT,(long)newBasketDoc.getInteger("output"));

    }


    private Projection createProjection(ProjectionBuilder builder, String projName) {

        return builder
                .named(projName)
                .projecting(TEST_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)
                .identifiedBy(event -> event.getBson().getString(projName))
                .as( (basket, event) -> event.getBson().put("output",projName) )
                .create();
    }


}

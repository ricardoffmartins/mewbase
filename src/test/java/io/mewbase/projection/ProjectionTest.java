package io.mewbase.projection;

import io.mewbase.MewbaseTestBase;
import io.mewbase.ServerTestBase;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonPath;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class ProjectionTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(ProjectionTest.class);

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

        final String TEST_BASKET_ID = "TestBasket";

        Projection projection = builder
                .named(TEST_PROJECTION_NAME)
                .projecting(TEST_CHANNEL)
                .onto(TEST_BINDER)
                .filteredBy(event -> true)      // is the default but exercise the method
                .identifiedBy(event -> event.getBson().getString(TEST_BASKET_ID))
                .as( (basket, event) -> {
                            assertNotNull(basket);
                            assertNotNull(event);
                            return event.getBson().put("output",true);
                        } )
                .create();

        assertNotNull(projection);
        assertEquals(TEST_PROJECTION_NAME, projection.getName());
    }



    @Test
    public void testSimpleProjectionRuns(TestContext testContext) throws Exception {

        // TODO Stuff from above

        // TODO use EventSink to send an events
        //Producer prod = client.createProducer(TEST_CHANNEL_1);
        //prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 10)).get();
    }

    @Test
    public void testProjectionNames() throws Exception {
        int numProjections = 10;
        for (int i = 0; i < numProjections; i++) {
            registerProjection("projection" + i);
        }
     //   List<String> names = server.listProjections();
      //  assertEquals(numProjections, names.size());
        for (int i = 0; i < numProjections; i++) {
          //  assertTrue(names.contains("projection" + i));
        }
    }

    @Test
    public void testGetProjection() throws Exception {
        int numProjections = 10;
        for (int i = 0; i < numProjections; i++) {
            registerProjection("projection" + i);
        }
        for (int i = 0; i < numProjections; i++) {
    //        Projection projection = server.getProjection("projection" + i);
//            assertNotNull(projection);
//            assertEquals("projection" + i, projection.getName());
        }
    }



    private Projection registerProjection(String projectionName) {
//        return server.buildProjection(projectionName).projecting(TEST_CHANNEL_1).onto(TEST_BINDER1)
//                .filteredBy(ev -> true).identifiedBy(ev -> ev.getString("basketID"))
//                .as((basket, del) ->
//                        BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID")))
//                .create();
        // Todo
        return null;
    }





}

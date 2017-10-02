package io.mewbase.projection;

import io.mewbase.MewbaseTestBase;
import io.mewbase.ServerTestBase;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.impl.nats.NatsEventSource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
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

    private static final String TEST_BINDER1 = "TestBinder";
    private static final String TEST_PROJECTION_NAME1 = "TestProjection";

    private static final String TEST_BASKET_ID = "basket1234";


    @Test
    public void testProjectionFactory() throws Exception {

        BinderStore store = new LmdbBinderStore();
        EventSource source = new NatsEventSource();

        ProjectionFactory factory = ProjectionFactory.instance(source,store);
        assertNotNull(factory);
        ProjectionBuilder builder = factory.builder();
        assertNotNull(builder);

        store.close();
        source.close();
    }



    @Test
    public void testSimpleProjection() throws Exception {
        registerProjection();
        //Producer prod = client.createProducer(TEST_CHANNEL_1);
        //prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 10)).get();
        waitUntilNumItems(10);
    }

    @Test
    public void testProjectionRestart() throws Exception {
        testProjectionRestart(false);
    }

    @Test
    public void testProjectionRestartWithDuplicates() throws Exception {
        testProjectionRestart(true);
    }

    @Test
    public void testPauseResume(TestContext testContext) throws Exception {
        AtomicInteger cnt = new AtomicInteger();
        AtomicReference<Projection> projRef = new AtomicReference<>();
        AtomicBoolean paused = new AtomicBoolean();
//        Projection projection = server.buildProjection(TEST_PROJECTION_NAME1).projecting(TEST_CHANNEL_1)
//                .onto(TEST_BINDER1).filteredBy(ev -> true).identifiedBy(ev -> ev.getString("basketID"))
//                .as((basket, del) -> {
//                    testContext.assertFalse(paused.get());
//                    if (cnt.incrementAndGet() == 5) {
//                        projRef.get().pause();
//                        paused.set(true);
//                        vertx.setTimer(200, tid -> {
//                            paused.set(false);
//                            projRef.get().resume();
//                        });
//                    }
//                    return BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID"));
//                })
//                .create();
  //      projRef.set(projection);
       // Producer prod = client.createProducer(TEST_CHANNEL_1);
       // prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 10)).get();
        waitUntilNumItems(10);
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

    private void testProjectionRestart(boolean duplicates) throws Exception {

        registerProjection();

       // Producer prod = client.createProducer(TEST_CHANNEL_1);

//        for (int i = 0; i < 10; i++) {
//            prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 1)).get();
//        }

        waitUntilNumItems(10);

        // Projection has processed all the events, now restart
   //     restart();

        if (duplicates) {
            // We reset the durable seq last acked so we get redeliveries - the duplicate detection should
            // ignore them
//            BsonObject lastSeqs = client.findByID(ServerImpl.DURABLE_SUBS_BINDER_NAME, TEST_PROJECTION_NAME1).get();
//            lastSeqs.put("lastAcked", 0);
//            ((ServerImpl)server).getDurableSubsBinder().put(TEST_PROJECTION_NAME1, lastSeqs).get();
        }

        registerProjection();

        // Wait a bit
        Thread.sleep(500);

//        BsonObject basket = client.findByID(TEST_BINDER1, TEST_BASKET_ID).get();
//        assertEquals(10, (int)basket.getBsonObject("products").getInteger("prod1"));
    }

    private Projection registerProjection() {
        return registerProjection(TEST_PROJECTION_NAME1);
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


    private void waitUntilNumItems(int numItems) {
        waitUntil(() -> {
            try {
//                BsonObject basket = client.findByID(TEST_BINDER1, TEST_BASKET_ID).get();
//                if (basket != null && basket.getBsonObject("products").getInteger("prod1") == numItems) {
//                    return true;
//                } else {
//                    return false;
                return true;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


}

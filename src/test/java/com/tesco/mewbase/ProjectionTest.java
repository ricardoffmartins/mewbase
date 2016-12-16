package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.bson.BsonPath;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.projection.Projection;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.ServerImpl;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class ProjectionTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(ProjectionTest.class);

    private static final String TEST_PROJECTION_NAME1 = "testproj";
    private static final String TEST_BASKET_ID = "basket1234";

    @Test
    public void testSimpleProjection() throws Exception {
        registerProjection();
        Producer prod = client.createProducer(TEST_CHANNEL_1);
        prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 10)).get();
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
        Projection projection = server.admin().buildProjection(TEST_PROJECTION_NAME1).projecting(TEST_CHANNEL_1)
                .onto(TEST_BINDER1).filteredBy(ev -> true).identifiedBy(ev -> ev.getString("basketID"))
                .as((basket, del) -> {
                    testContext.assertFalse(paused.get());
                    if (cnt.incrementAndGet() == 5) {
                        projRef.get().pause();
                        paused.set(true);
                        vertx.setTimer(200, tid -> {
                            paused.set(false);
                            projRef.get().resume();
                        });
                    }
                    return BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID"));
                })
                .create();
        projRef.set(projection);
        Producer prod = client.createProducer(TEST_CHANNEL_1);
        prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 10)).get();
        waitUntilNumItems(10);
    }

    @Test
    public void testProjectionNames() throws Exception {
        int numProjections = 10;
        for (int i = 0; i < numProjections; i++) {
            registerProjection("projection" + i);
        }
        Set<String> names = server.admin().getProjectionNames();
        assertEquals(numProjections, names.size());
        for (int i = 0; i < numProjections; i++) {
            assertTrue(names.contains("projection" + i));
        }
    }

    private void testProjectionRestart(boolean duplicates) throws Exception {

        registerProjection();

        Producer prod = client.createProducer(TEST_CHANNEL_1);

        for (int i = 0; i < 10; i++) {
            prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 1)).get();
        }

        waitUntilNumItems(10);

        // Projection has processed all the events, now restart
        client.close();
        server.stop().get();

        ServerOptions serverOptions = createServerOptions(logDir);
        ClientOptions clientOptions = createClientOptions();

        server = Server.newServer(vertx, serverOptions);
        server.start().get();
        setupChannelsAndBinders();

        client = Client.newClient(vertx, clientOptions);

        if (duplicates) {
            // We reset the durable seq last acked so we get redeliveries - the duplicate detection should
            // ignore them
            BsonObject lastSeqs = client.findByID(ServerImpl.DURABLE_SUBS_BINDER_NAME, TEST_PROJECTION_NAME1).get();
            lastSeqs.put("lastAcked", 0);
            ((ServerImpl)server).docManager().put(ServerImpl.DURABLE_SUBS_BINDER_NAME, TEST_PROJECTION_NAME1, lastSeqs).get();
        }

        registerProjection();

        // Wait a bit
        Thread.sleep(500);

        BsonObject basket = client.findByID(TEST_BINDER1, TEST_BASKET_ID).get();
        assertEquals(10, (int)basket.getBsonObject("products").getInteger("prod1"));
    }

    private void sendEvents(int num) throws Exception {
        Producer prod = client.createProducer(TEST_CHANNEL_1);
        for (int i = 0; i < num; i++) {
            prod.publish(new BsonObject().put("basketID", TEST_BASKET_ID).put("productID", "prod1").put("quantity", 1)).get();
        }
    }

    private Projection registerProjection() {
        return registerProjection(TEST_PROJECTION_NAME1);
    }

    private Projection registerProjection(String projectionName) {
        return server.admin().buildProjection(projectionName).projecting(TEST_CHANNEL_1).onto(TEST_BINDER1)
                .filteredBy(ev -> true).identifiedBy(ev -> ev.getString("basketID"))
                .as((basket, del) ->
                        BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID")))
                .create();
    }

    private void waitUntilNumItems(int numItems) {
        waitUntil(() -> {
            try {
                BsonObject basket = client.findByID(TEST_BINDER1, TEST_BASKET_ID).get();
                if (basket != null && basket.getBsonObject("products").getInteger("prod1") == numItems) {
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


}

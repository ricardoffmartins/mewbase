package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.Producer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Jamie on 14/10/2016.
 */
@RunWith(VertxUnitRunner.class)
public class QueryTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(QueryTest.class);

    protected Producer prod;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        installInsertProjection();
        prod = client.createProducer(TEST_CHANNEL_1);
    }

    @Override
    protected void setupChannelsAndBinders() throws Exception {
        server.admin().createChannel(TEST_CHANNEL_1).get();
        server.admin().createBinder(TEST_BINDER1).get();
    }

    @Test
    public void testGetById() throws Exception {
        String docID = getID(123);
        BsonObject doc = new BsonObject().put("id", docID).put("foo", "bar");
        prod.publish(doc).get();

        BsonObject received = waitForDoc(123);

        Assert.assertEquals(docID, received.getString("id"));
        Assert.assertEquals("bar", received.getString("foo"));
    }

    @Test
    public void testNoSuchBinder() throws Exception {

        BsonObject doc = client.findByID("nobinder", "xgcxgcxgc").get();
    }

    @Test
    public void testFindMatching(TestContext context) throws Exception {
        int numDocs = 100;
        for (int i = 0; i < numDocs; i++) {
            String docID = getID(i);
            BsonObject doc = new BsonObject().put("id", docID).put("foo", "bar");
            prod.publish(doc).get();
        }

        waitForDoc(numDocs - 1);

        Async async = context.async();
        AtomicInteger cnt = new AtomicInteger();
        client.findMatching(TEST_BINDER1, new BsonObject(), qr -> {
            String expectedID = getID(cnt.getAndIncrement());
            context.assertEquals(expectedID, qr.document().getString("id"));
            if (cnt.get() == numDocs) {
                context.assertTrue(qr.isLast());
                async.complete();
            } else {
                context.assertFalse(qr.isLast());
            }
        }, t -> context.fail("Exception shouldn't be received"));
    }

    @Test
    public void testGetByIdReturnsNullForNonExistentDocument(TestContext context) throws Exception {
        BsonObject doc = client.findByID(TEST_BINDER1, "non-existent-document").get();
        Assert.assertEquals(null, doc);
    }

    @Test
    public void testFindMatchingNoConnect(TestContext context) throws Exception {
        Client client2 = Client.newClient(vertx, new ClientOptions().setHost("uiqhwiuqwdui"));
        Async async = context.async();
        client2.findMatching(TEST_BINDER1, new BsonObject(), qr -> {
            context.fail("should not be called");
        }, t -> async.complete());
        client2.close();
    }

    @Test
    public void testFindMatchingNoDocuments(TestContext context) throws Exception {
        Async async = context.async();
        client.findMatching(TEST_BINDER1, new BsonObject(), qr -> {
            context.assertNull(qr.document());
            context.assertTrue(qr.isLast());
            async.complete();
        }, t -> context.fail("Exception shouldn't be received"));
    }

    protected BsonObject waitForDoc(int docID) {
        // Wait until docs are inserted
        return waitForNonNull(() -> {
            try {
                return client.findByID(TEST_BINDER1, getID(docID)).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


    protected void installInsertProjection() {
        server.admin().buildProjection("testproj").projecting(TEST_CHANNEL_1).onto(TEST_BINDER1).filteredBy(ev -> true)
                .identifiedBy(ev -> ev.getString("id"))
                .as((basket, del) -> del.event()).create();
    }

    protected String getID(int id) {
        return String.format("id-%05d", id);
    }

}

package io.mewbase;

import io.mewbase.bson.BsonObject;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

/**
 * Created by Jamie on 14/10/2016.
 */
@RunWith(VertxUnitRunner.class)
public class QueryTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(QueryTest.class);

    // TODO - Replace with Event Source
    // protected Producer prod;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        installInsertProjection();
       // prod = client.createProducer(TEST_CHANNEL_1);
    }


    protected void setupBinders() throws Exception {
        server.createBinder(TEST_BINDER1).get();
    }

    @Test
    public void testGetById() throws Exception {
        String docID = getID(123);
        BsonObject doc = new BsonObject().put("id", docID).put("foo", "bar");
        //prod.publish(doc).get();

//        BsonObject received = waitForDoc(123);
//
//        assertEquals(docID, received.getString("id"));
//        assertEquals("bar", received.getString("foo"));
    }

    @Test
    public void testNoSuchBinder() throws Exception {
//        try {
//           // BsonObject doc = client.findByID("nobinder", "xgcxgcxgc").get();
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
            // OK
//            Exception me = (MewException)e.getCause();
//            assertEquals("No such binder nobinder", me.getMessage());
//        }
    }

    @Test
    //@Repeat(value = 1000)
    public void testExecuteQuery(TestContext context) throws Exception {
        int numDocs = 100;
        for (int i = 0; i < numDocs; i++) {
            String docID = getID(i);
            BsonObject doc = new BsonObject().put("id", docID).put("foo", "bar");
         //   prod.publish(doc).get();
        }

       // waitForDoc(numDocs - 1);

        // Setup a query
        server.buildQuery("testQuery").documentFilter((doc, ctx) -> {
            return true;
        }).from(TEST_BINDER1).create();

        Async async = context.async();
        AtomicInteger cnt = new AtomicInteger();
//        client.executeQuery("testQuery", new BsonObject(), qr -> {
//            String expectedID = getID(cnt.getAndIncrement());
//            context.assertEquals(expectedID, qr.document().getString("id"));
//            if (cnt.get() == numDocs) {
//                context.assertTrue(qr.isLast());
//                async.complete();
//            } else {
//                context.assertFalse(qr.isLast());
//            }
//        }, t -> context.fail("Exception shouldn't be received"));
//

        Thread.sleep(100);
    }


    // TODO more query tests

    @Test
    public void testGetByIdReturnsNullForNonExistentDocument(TestContext context) throws Exception {
//        BsonObject doc = client.findByID(TEST_BINDER1, "non-existent-document").get();
//        assertEquals(null, doc);
    }

//    protected BsonObject waitForDoc(int docID) {
        // Wait until docs are inserted
//        return waitForNonNull(() -> {
//            try {
//                return client.findByID(TEST_BINDER1, getID(docID)).get();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        });
//    }


    protected void installInsertProjection() {
        server.buildProjection("testproj").projecting(TEST_CHANNEL_1).onto(TEST_BINDER1).filteredBy(ev -> true)
                .identifiedBy(ev -> ev.getString("id"))
                .as((basket, del) -> del.event()).create();
    }

    protected String getID(int id) {
        return String.format("id-%05d", id);
    }

}

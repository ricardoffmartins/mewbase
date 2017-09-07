package io.mewbase.binder;

import io.mewbase.ServerTestBase;
import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.mewbase.server.Binder;
import io.mewbase.server.DocReadStream;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertFalse;

import static org.junit.Assert.*;

/**
 * TODO No Longer a client so tests need to be reinstated with REST client proxy
 * <p>
 * Created by tim on 14/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class BindersTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(BindersTest.class);

    protected DocReadStream stream;
    protected Binder testBinder1;

    @Override
    protected void tearDown(TestContext context) throws Exception {
        if (stream != null) {
            stream.close();
        }
        super.tearDown(context);
    }


    protected void setupBinders() throws Exception {
        server.createBinder(TEST_BINDER1).get();
        server.createBinder(TEST_BINDER2).get();
        testBinder1 = server.getBinder(TEST_BINDER1);
    }

    @Test
    public void testListBinders() throws Exception {
        int numBinders = 10;
        CompletableFuture[] all = new CompletableFuture[numBinders];
        for (int i = 0; i < numBinders; i++) {
            all[i] = server.createBinder("testbinder" + i);
        }
        CompletableFuture.allOf(all).get();
      //  BsonArray binders1 = client.listBinders().get();

//        Set<String> bindersSet1 = new HashSet<>(binders1.getList());
//        for (int i = 0; i < numBinders; i++) {
//            assertTrue(bindersSet1.contains("testbinder" + i));
//        }

        final String otherBinderName = "someotherbinder";

        // Create a new one
        server.createBinder(otherBinderName).get();
//        BsonArray binders2 = client.listBinders().get();
//        Set<String> bindersSet2 = new HashSet<>(binders2.getList());
//        assertTrue(bindersSet2.contains(otherBinderName));
//        assertEquals(bindersSet1.size() + 1, bindersSet2.size());
    }

    @Test
    public void testCreateBinder() throws Exception {
        final String binderName = "somebinder";
//        CompletableFuture<Boolean> cf = client.createBinder(binderName);
//        assertTrue(cf.get());
//
//        List<String> binderNames = server.listBinders();
//        Set<String> bindersSet = new HashSet<>(binderNames);
//        assertTrue(bindersSet.contains(binderName));
//
//        CompletableFuture<Boolean> cf2 = client.createBinder(binderName);
//        assertFalse(cf2.get());
    }

    @Test
    public void testSimplePutGet() throws Exception {
        BsonObject docPut = createObject();
        assertNull(testBinder1.put("id1234", docPut).get());
        BsonObject docGet = testBinder1.get("id1234").get();
        assertEquals(docPut, docGet);
    }

    @Test
    public void testFindNoEntry() throws Exception {
        assertNull(testBinder1.get("id1234").get());
    }

    @Test
    public void testDelete() throws Exception {
        BsonObject docPut = createObject();
        assertNull(testBinder1.put("id1234", docPut).get());
        BsonObject docGet = testBinder1.get("id1234").get();
        assertEquals(docPut, docGet);
        assertTrue(testBinder1.delete("id1234").get());
        docGet = testBinder1.get("id1234").get();
        assertNull(docGet);
    }

    @Test
    public void testPutGetDifferentBinders() throws Exception {
        createBinder("binder1");
        createBinder("binder2");
        Binder binder1 = server.getBinder("binder1");
        Binder binder2 = server.getBinder("binder2");

        BsonObject docPut1 = createObject();
        docPut1.put("binder", "binder1");
        assertNull(binder1.put("id0", docPut1).get());

        BsonObject docPut2 = createObject();
        docPut2.put("binder", "binder2");
        assertNull(binder2.put("id0", docPut2).get());

        BsonObject docGet1 = binder1.get("id0").get();
        assertEquals("binder1", docGet1.remove("binder"));

        BsonObject docGet2 = binder2.get("id0").get();
        assertEquals("binder2", docGet2.remove("binder"));

    }

    @Test
    public void testPutGetMultiple() throws Exception {
        int numDocs = 10;
        int numBinders = 10;
        for (int i = 0; i < numBinders; i++) {
            String binderName = "pgmbinder" + i;
            createBinder(binderName);
            Binder binder = server.getBinder(binderName);
            for (int j = 0; j < numDocs; j++) {
                BsonObject docPut = createObject();
                docPut.put("binder", binderName);
                assertNull(binder.put("id" + j, docPut).get());
            }
        }
        for (int i = 0; i < numBinders; i++) {
            String binderName = "pgmbinder" + i;
            Binder binder = server.getBinder(binderName);
            for (int j = 0; j < numDocs; j++) {
                BsonObject docGet = binder.get("id" + j).get();
                assertEquals(binderName, docGet.remove("binder"));
            }
        }
    }

    @Test
    public void testRestart() throws Exception {
        BsonObject docPut = createObject();
        assertNull(testBinder1.put("id1234", docPut).get());
        BsonObject docGet = testBinder1.get("id1234").get();
        assertEquals(docPut, docGet);
        // We don't use the restart() method as this recreates the binders
        stopServer();
        startServer();
        testBinder1 = server.getBinder(TEST_BINDER1);
        docGet = testBinder1.get("id1234").get();
        assertEquals(docPut, docGet);
    }

    @Test
    public void testStream(TestContext testContext) throws Exception {

        // Add some docs
        int numDocs = 100;
        addDocs(TEST_BINDER1, numDocs);

        // Add some docs in another binder
        addDocs(TEST_BINDER2, numDocs);

        stream = testBinder1.getMatching(doc -> true);

        Async async = testContext.async();

        AtomicInteger docCount = new AtomicInteger();
        stream.handler(doc -> {
            int expectedNum = docCount.getAndIncrement();
            int docNum = doc.getInteger("docNum");
            assertEquals(expectedNum, docNum);
            if (expectedNum == numDocs - 1) {
                async.complete();
            }
        });

        stream.start();
    }

    @Test
    public void testStreamWithFilter(TestContext testContext) throws Exception {

        // Add some docs
        int numDocs = 100;
        addDocs(TEST_BINDER1, numDocs);

        // Add some docs in another binder
        addDocs(TEST_BINDER2, numDocs);

        stream = testBinder1.getMatching(doc -> {
            int docNum = doc.getInteger("docNum");
            return docNum >= 10 && docNum < 90;
        });

        Async async = testContext.async();

        AtomicInteger docCount = new AtomicInteger(10);
        stream.handler(doc -> {
            int expectedNum = docCount.getAndIncrement();
            int docNum = doc.getInteger("docNum");
            assertEquals(expectedNum, docNum);
            if (expectedNum == 89) {
                async.complete();
            }
        });

        stream.start();
    }

    @Test
    public void testStreamPauseResume(TestContext testContext) throws Exception {

        // Add some docs
        int numDocs = 100;
        addDocs(TEST_BINDER1, numDocs);

        // Add some docs in another binder
        addDocs(TEST_BINDER2, numDocs);

        stream = testBinder1.getMatching(doc -> true);

        Async async = testContext.async();

        AtomicInteger docCount = new AtomicInteger();
        AtomicBoolean paused = new AtomicBoolean();
        stream.handler(doc -> {
            testContext.assertFalse(paused.get());
            int expectedNum = docCount.getAndIncrement();
            int docNum = doc.getInteger("docNum");
            assertEquals(expectedNum, docNum);

            if (expectedNum == numDocs / 2) {
                stream.pause();
                paused.set(true);
                vertx.setTimer(100, tid -> {
                    paused.set(false);
                    stream.resume();
                });
            }
            if (expectedNum == numDocs - 1) {
                async.complete();
            }
        });

        stream.start();
    }

    // TODO
    // Test binders in binders_binder but not in actual storage and vice versa
    // etc

    private void addDocs(String binderName, int numDocs) throws Exception {
        for (int i = 0; i < numDocs; i++) {
            BsonObject docPut = createObject();
            docPut.put("docNum", i);
            Binder binder = server.getBinder(binderName);
            assertNull(binder.put(getID(i), docPut).get());
        }
    }

    private String getID(int id) {
        return String.format("id-%05d", id);
    }

    protected BsonObject createObject() {
        BsonObject obj = new BsonObject();
        obj.put("foo", "bar").put("quux", 1234).put("wib", true);
        return obj;
    }

    protected boolean createBinder(String binderName) throws Exception {
        return server.createBinder(binderName).get();
    }

}

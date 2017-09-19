package io.mewbase.binder;

import io.mewbase.MewbaseTestBase;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.binders.Binder;


import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;


import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

/**
 * <p>
 * Created by tim on 14/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class BindersTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(BindersTest.class);

    private final static String BINDER_NAME = "TestBinderName";


    @Test
    public void testCreateBinderStore() throws Exception {
        BinderStore store = new LmdbBinderStore(createMewbaseOptions());
        store.binderNames().forEach( bn-> System.out.println(bn));
        assertEquals(store.binderNames().count(),0L);
        store.close();
    }


    @Test
    public void testOpenBinders() throws Exception {

        // set up the store and add some binders
        BinderStore store = new LmdbBinderStore(createMewbaseOptions());

        final int numBinders = 10;
        CompletableFuture[] all = new CompletableFuture[numBinders];
        for (int i = 0; i < numBinders; i++) {
            all[i] = store.open("testbinder" + i);
        }
        CompletableFuture.allOf(all).get();

        Set<String> bindersSet1 = store.binderNames().collect(toSet());
        for (int i = 0; i < numBinders; i++) {
            assertTrue(bindersSet1.contains("testbinder" + i));
        }

        final String name = "AnotherBinder";
        store.open(name).get();
        Set<String> bindersSet2 = store.binderNames().collect(toSet());
        assertTrue(bindersSet2.contains(name));
        assertEquals(bindersSet1.size() + 1, bindersSet2.size());
    }


   @Test
   public void testSimplePutGet() throws Exception {
       BinderStore store = new LmdbBinderStore(createMewbaseOptions());
       Binder binder = store.open(BINDER_NAME).get();
       BsonObject docPut = createObject();
       assertNull(binder.put("id1234", docPut).get());
       BsonObject docGet = binder.get("id1234").get();
       assertEquals(docPut, docGet);
    }

    @Test
    public void testFindNoEntry() throws Exception {
        BinderStore store = new LmdbBinderStore(createMewbaseOptions());
        Binder binder = store.open(BINDER_NAME).get();
        assertNull(binder.get("id1234").get());
    }

    @Test
    public void testDelete() throws Exception {
//        BsonObject docPut = createObject();
//        assertNull(testBinder1.put("id1234", docPut).get());
//        BsonObject docGet = testBinder1.get("id1234").get();
//        assertEquals(docPut, docGet);
//        assertTrue(testBinder1.delete("id1234").get());
//        docGet = testBinder1.get("id1234").get();
//        assertNull(docGet);
    }

    @Test
    public void testPutGetDifferentBinders() throws Exception {
//        createBinder("binder1");
//        createBinder("binder2");
//        Binder binder1 = server.getBinder("binder1");
//        Binder binder2 = server.getBinder("binder2");
//
//        BsonObject docPut1 = createObject();
//        docPut1.put("binder", "binder1");
//        assertNull(binder1.put("id0", docPut1).get());
//
//        BsonObject docPut2 = createObject();
//        docPut2.put("binder", "binder2");
//        assertNull(binder2.put("id0", docPut2).get());
//
//        BsonObject docGet1 = binder1.get("id0").get();
//        assertEquals("binder1", docGet1.remove("binder"));
//
//        BsonObject docGet2 = binder2.get("id0").get();
//        assertEquals("binder2", docGet2.remove("binder"));

    }

    @Test
    public void testPutGetMultiple() throws Exception {
//        int numDocs = 10;
//        int numBinders = 10;
//        for (int i = 0; i < numBinders; i++) {
//            String binderName = "pgmbinder" + i;
//            createBinder(binderName);
//            Binder binder = server.getBinder(binderName);
//            for (int j = 0; j < numDocs; j++) {
//                BsonObject docPut = createObject();
//                docPut.put("binder", binderName);
//                assertNull(binder.put("id" + j, docPut).get());
//            }
//        }
//        for (int i = 0; i < numBinders; i++) {
//            String binderName = "pgmbinder" + i;
//            Binder binder = server.getBinder(binderName);
//            for (int j = 0; j < numDocs; j++) {
//                BsonObject docGet = binder.get("id" + j).get();
//                assertEquals(binderName, docGet.remove("binder"));
//            }
//        }
    }



    private String getID(int id) {
        return String.format("id-%05d", id);
    }

    protected BsonObject createObject() {
        BsonObject obj = new BsonObject();
        obj.put("foo", "bar").put("quux", 1234).put("wib", true);
        return obj;
    }



}

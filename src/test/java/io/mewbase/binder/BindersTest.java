package io.mewbase.binder;

import io.mewbase.MewbaseTestBase;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.binders.Binder;


import io.mewbase.server.MewbaseOptions;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;


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
        IntStream.range(0, all.length).forEach( i -> {
            all[i] = store.open("testbinder" + i);
        });
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
        store.close();
    }


   @Test
   public void testSimplePutGet() throws Exception {
       BinderStore store = new LmdbBinderStore(createMewbaseOptions());
       Binder binder = store.open(BINDER_NAME).get();
       BsonObject docPut = createObject();
       assertNull(binder.put("id1234", docPut).get());
       BsonObject docGet = binder.get("id1234").get();
       assertEquals(docPut, docGet);
       store.close();
    }

    @Test
    public void testFindNoEntry() throws Exception {
        BinderStore store = new LmdbBinderStore(createMewbaseOptions());
        Binder binder = store.open(BINDER_NAME).get();
        assertNull(binder.get("id1234").get());
        store.close();
    }

    @Test
    public void testDelete() throws Exception {
        BinderStore store = new LmdbBinderStore(createMewbaseOptions());
        Binder binder = store.open(BINDER_NAME).get();

        BsonObject docPut = createObject();
        assertNull(binder.put("id1234", docPut).get());
        BsonObject docGet = binder.get("id1234").get();
        assertEquals(docPut, docGet);
        assertTrue(binder.delete("id1234").get());
        docGet = binder.get("id1234").get();
        assertNull(docGet);
        store.close();
    }


    @Test
    public void testGetIds() throws Exception {

        BinderStore store = new LmdbBinderStore(createMewbaseOptions());
        Binder binder = store.open(BINDER_NAME).get();

        final int MANY_DOCS = 64;
        final String DOC_ID_KEY = "id";

        final IntStream range = IntStream.rangeClosed(1, MANY_DOCS);

        range.forEach(i -> {
            final BsonObject docPut = createObject();
            binder.put(String.valueOf(i), docPut.put(DOC_ID_KEY, i));
        });

        Consumer<String> checker = (String id) -> {
            try {
                BsonObject b = binder.get(id).get();
                assertNotNull(b);
                assertEquals((int)b.getInteger(DOC_ID_KEY) , (int)Integer.parseInt(id));
            } catch (Exception e) {
                fail(e.getMessage());
            }
        };

        // get all
        Stream<String> ids = binder.getIds().get();
        ids.forEach(checker);

        // get some of the docs
        final int HALF_THE_DOCS = MANY_DOCS / 2;
        Stream<String> some = binder.getIdsWithFilter(bson -> {
            try {
                return bson.getInteger(DOC_ID_KEY) <= HALF_THE_DOCS;
            } catch (Exception e) {
                fail(e.getMessage());
            }
            return false;
        }).get();

        assertEquals(some.collect(toSet()).size(), HALF_THE_DOCS);
        store.close();
    }


    @Test
    public void testPutGetDifferentBinders() throws Exception {

        final String B1 = BINDER_NAME + "1";
        final String B2 = BINDER_NAME + "2";
        BinderStore store = new LmdbBinderStore(createMewbaseOptions());
        Binder binder1 = store.open(B1).get();
        Binder binder2 = store.open(B2).get();

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
        store.close();
    }

    @Test
    public void testBinderIsPersistent() throws Exception {

        final MewbaseOptions OPTIONS = createMewbaseOptions();

        BinderStore store = new LmdbBinderStore(OPTIONS);
        Binder binder = store.open(BINDER_NAME).get();
        BsonObject docPut = createObject();
        binder.put("id1234", docPut).get();
        store.close();

        Thread.sleep(10);

        BinderStore store2 = new LmdbBinderStore(OPTIONS);
        Binder binder2 = store2.open(BINDER_NAME).get();
        BsonObject docGet = binder2.get("id1234").get();
        assertEquals(docPut, docGet);
        store2.close();
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

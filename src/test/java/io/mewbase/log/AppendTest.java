package io.mewbase.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.impl.log.HeaderOps;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class AppendTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(AppendTest.class);

    @Test
    public void testAppend() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length() + HeaderOps.HEADER_OFFSET;
        int numObjects = 100;
        serverOptions = origServerOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1);
        startLog();
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertExists(0);
        assertLogChunkLength(0, length * numObjects);
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
        });
    }

    @Test
    public void testAppendNextFile() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length() + HeaderOps.HEADER_OFFSET;
        int numObjects = 100;
        // space is reserved for header so will overspill without subtracting 1
        serverOptions = origServerOptions().setMaxLogChunkSize(length * numObjects).setMaxRecordSize(length + 1);
        startLog();
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertExists(0);
        assertLogChunkLength(0, serverOptions.getMaxLogChunkSize());

        AtomicInteger loadedCount = new AtomicInteger(0);
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects - 1);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
            loadedCount.incrementAndGet();
        });

        assertExists(1);
        assertLogChunkLength(1, length);
        assertEquals(numObjects - 1, loadedCount.get());

        assertObjects(1, (cnt, record) -> {
            assertTrue(cnt < 1);
            BsonObject expected = obj.copy().put("num", 99);
            assertTrue(expected.equals(record));
        });
    }

    @Test
    public void testAppendConcurrent() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length() + HeaderOps.HEADER_OFFSET;
        int numObjects = 100;
        serverOptions = origServerOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1);
        startLog();
        appendObjectsConcurrently(numObjects, i -> obj.copy().put("num", i));
        assertExists(0);
        assertLogChunkLength(0, length * numObjects);
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
        });
    }

    @Test
    public void testPrealloc() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length() + HeaderOps.HEADER_OFFSET;
        int numObjects = 100;
        int preallocSize = 10 * length;
        serverOptions = origServerOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1).setPreallocateSize(preallocSize);
        startLog();
        assertExists(0);
        assertLogChunkLength(0, preallocSize);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
        });
        assertLogChunkLength(0, length * numObjects);
    }

    @Test
    public void testPreallocNextFile() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length() + HeaderOps.HEADER_OFFSET;
        int numObjects = 100;
        int preallocSize = 10 * length;
        serverOptions = origServerOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1).setPreallocateSize(preallocSize);
        startLog();
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertExists(1);
        assertLogChunkLength(1, preallocSize);
    }


    protected void assertObjects(int fileNumber, BiConsumer<Integer, BsonObject> objectConsumer) throws Exception {
        File file = new File(logsDir, getLogFileName(TEST_CHANNEL_1, fileNumber));
        assertTrue(file.exists());
        Buffer buff = readFileIntoBuffer(file);
        int pos = 0;
        int count = 0;
        while (true) {
            final int objStart = HeaderOps.HEADER_OFFSET + pos;
            int objLen = buff.getIntLE(objStart);
            if (objLen == 0) {
                break;
            }
            Buffer objBuff = buff.slice(objStart, objStart + objLen);
            BsonObject record = new BsonObject(objBuff);
            objectConsumer.accept(count, record);
            count++;
            pos += HeaderOps.HEADER_OFFSET + objLen;
            if (pos >= file.length()) {
                break;
            }
        }
    }


}

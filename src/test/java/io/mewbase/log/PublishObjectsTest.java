package io.mewbase.log;


import io.mewbase.bson.BsonObject;
import io.mewbase.server.impl.Protocol;
import io.mewbase.server.impl.log.FramingOps;
import io.mewbase.server.impl.log.HeaderOps;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class PublishObjectsTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(PublishObjectsTest.class);

    private final static int MAX_RECORD_SIZE = 100;
    private final static int LOG_CHUNK_SIZE = 10 * 1024;
    // framing and timestamp etc etc
    private final static int RECORD_BYTE_COST  = 51; // Bytes


    @Test
    //@Repeat(value =10000)
    public void testPublish() throws Exception {

        final int numRecords = 100;
        BsonObject event = new BsonObject().put("foo", "bar").put("num", 0);
        final int recordSize =  event.encode().length() + RECORD_BYTE_COST;
        final int expChunkLength = HeaderOps.HEADER_SIZE + (numRecords * recordSize) ;

        serverOptions = origServerOptions().
                        setMaxLogChunkSize(LOG_CHUNK_SIZE).
                        setMaxRecordSize(MAX_RECORD_SIZE);

        startLog();
        publishObjectsSequentially(numRecords, i -> event.copy().put("num", i));

        assertExists(0);
        assertLogChunkLength(0, expChunkLength);
        assertObjects(0, (cnt, object) -> {
            assertTrue(cnt < numRecords);
            BsonObject expected = event.copy().put("num", cnt);
            assertTrue(expected.equals(object));
        });
    }

    @Test
    public void testPublishIntoNextFile() throws Exception {

        int numObjects = 100;
        BsonObject event = new BsonObject().put("foo", "bar").put("num", 0);
        final int recordSize = event.encode().length() + RECORD_BYTE_COST;
        // make a record spill into the next file.
        final int fileChunkSize = HeaderOps.HEADER_SIZE + ( (numObjects - 1) * recordSize );
        // hence
        final int expectedNextFileSize = HeaderOps.HEADER_SIZE + recordSize;

        serverOptions = origServerOptions().
                        setMaxLogChunkSize(fileChunkSize).
                        setMaxRecordSize(MAX_RECORD_SIZE);

        startLog();
        publishObjectsSequentially(numObjects, i -> event.copy().put("num", i));

        assertExists(0);
        assertLogChunkLength(0, serverOptions.getMaxLogChunkSize());

        AtomicInteger loadedCount = new AtomicInteger(0);
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects - 1);
            BsonObject expected = event.copy().put("num", cnt);
            assertTrue(expected.equals(record));
            loadedCount.incrementAndGet();
        });

        assertExists(1);
        assertLogChunkLength(1, expectedNextFileSize);
        assertEquals(numObjects - 1, loadedCount.get());

        assertObjects(1, (cnt, record) -> {
            assertTrue(cnt < 1);
            BsonObject expected = event.copy().put("num", 99);
            assertTrue(expected.equals(record));
        });
    }

    @Test
    public void testPublishConcurrent() throws Exception {

        final int numRecords = 100;
        BsonObject event = new BsonObject().put("foo", "bar").put("num", 0);
        final int recordSize =  event.encode().length() + RECORD_BYTE_COST;
        final int expChunkLength = HeaderOps.HEADER_SIZE + (numRecords * recordSize) ;

        serverOptions = origServerOptions().
                setMaxLogChunkSize(LOG_CHUNK_SIZE).
                setMaxRecordSize(MAX_RECORD_SIZE);

        startLog();
        List<Long> orderedResults = publishObjectsConcurrently(numRecords, i -> event.copy().put("num", i));
        if (orderedResults.get(0) != 0L)  System.out.println(orderedResults);
        assertExists(0);
        assertLogChunkLength(0, expChunkLength);
        //System.out.println(orderedResults);
        Iterator<Long> itr = orderedResults.iterator();
        assertObjects(0, (recordNum, record) -> {
            assertTrue(recordNum < numRecords);
            BsonObject expected = event.copy().put("num", recordNum);
            //System.out.println(expected + "->" + record);
            assertTrue(expected.equals(record));
        });
        Thread.sleep(1000);
    }

    @Test
    public void testPrealloc() throws Exception {
        final int numRecords = 100;
        BsonObject event = new BsonObject().put("foo", "bar").put("num", 0);
        final int recordSize =  event.encode().length() + RECORD_BYTE_COST;
        final int expChunkLength = HeaderOps.HEADER_SIZE + (numRecords * recordSize) ;

        int preallocSize = 10 * recordSize;

        serverOptions = origServerOptions().
                setMaxLogChunkSize(expChunkLength).
                setMaxRecordSize(MAX_RECORD_SIZE).
                setPreallocateSize(preallocSize);

        startLog();
        assertExists(0);
        assertLogChunkLength(0, preallocSize);

        publishObjectsSequentially(numRecords, i -> event.copy().put("num", i));
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numRecords);
            BsonObject expected = event.copy().put("num", cnt);
            assertTrue(expected.equals(record));
        });
        assertLogChunkLength(0, expChunkLength);
    }


    @Test
    public void testPreallocNextFile() throws Exception {

        final int numRecords = 100;
        BsonObject event = new BsonObject().put("foo", "bar").put("num", 0);
        final int recordSize =  event.encode().length() + RECORD_BYTE_COST;
        final int expChunkLength = HeaderOps.HEADER_SIZE + (numRecords * recordSize) ;

        final int preallocSize =  10 * recordSize;

        serverOptions = origServerOptions().
                setMaxLogChunkSize(expChunkLength).
                setMaxRecordSize(MAX_RECORD_SIZE).
                setPreallocateSize(preallocSize);

        startLog();
        publishObjectsSequentially(numRecords, i -> event.copy().put("num", i));
        assertExists(1);
        assertLogChunkLength(1, preallocSize);
    }



    protected void assertObjects(int fileNumber, BiConsumer<Integer, BsonObject> objectConsumer) throws Exception {
        File file = new File(logsDir, getLogFileName(TEST_CHANNEL_1, fileNumber));
        assertTrue(file.exists());
        Buffer buff = readFileIntoBuffer(file);
        int pos = HeaderOps.HEADER_SIZE;
        int count = 0;
        while (true) {
            final int objStart = FramingOps.CHECKSUM_SIZE + pos;
            int objLen = buff.getIntLE(objStart);
            if (objLen == 0) {
                break;
            }
            Buffer objBuff = buff.slice(objStart, objStart + objLen);
            BsonObject record = new BsonObject(objBuff);
            BsonObject event = record.getBsonObject(Protocol.RECEV_EVENT);
            objectConsumer.accept(count, event);
            count++;
            pos += FramingOps.FRAME_SIZE + objLen;
            if (pos >= file.length()) {
                break;
            }
        }
    }


}

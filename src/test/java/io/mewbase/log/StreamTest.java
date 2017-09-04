package io.mewbase.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.ServerOptions;
import io.mewbase.server.impl.log.FramingOps;
import io.mewbase.server.impl.log.HeaderOps;
import io.mewbase.server.impl.log.LogImpl;
import io.mewbase.server.impl.log.LogReadStreamImpl;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class StreamTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(StreamTest.class);

    private final static int RECORD_BYTE_COST = 31; // Bytes

    private BsonObject event = new BsonObject().put("foo", "bar").put("num", 0);
    private int recordLen = FramingOps.FRAME_SIZE + event.encode().length() + RECORD_BYTE_COST;
    private int numObjects = 100;

    @Test
    public void test_stream_single_file_less_than_max_file_size(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects + 10);
        int expectedFileLength = HeaderOps.HEADER_SIZE + (recordLen * numObjects);
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, recordLen,
                0, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_single_file_equal_to_max_file_size(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects ;
        int expectedFileLength = HeaderOps.HEADER_SIZE + (recordLen * numObjects);
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, recordLen,
                0, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_two_files_fill_first_exactly(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects - 1) ;
        int expectedFileLength = HeaderOps.HEADER_SIZE + recordLen;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, recordLen,
                1, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_two_files_fill_first_with_empty_space(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects - 1) + (recordLen / 2)  ;
        int expectedFileLength = HeaderOps.HEADER_SIZE + recordLen;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, recordLen,
                1, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_two_files_fill_both_exactly(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects / 2 ;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, recordLen,
                1, fileSize  , recordLen);
    }

    @Test
    public void test_stream_five_files_with_empty_space(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects / 5) + (recordLen / 2) ;
        int expectedFileLength = 1584;  // should have a relationship to above
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, recordLen,
                4, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_five_files_fill_all_exactly(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects / 5 ;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, recordLen,
                4, fileSize , recordLen);
    }

    @Test
    public void test_stream_single_file_less_than_max_file_size_small_rb(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects + 10) ;
        int expectedFileLength = HeaderOps.HEADER_SIZE + recordLen * numObjects;
        test_stream(testContext, numObjects, fileSize, recordLen - 1, recordLen,
                0, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_single_file_equal_to_max_file_size_small_rb(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects ;
        test_stream(testContext, numObjects, fileSize, recordLen - 1, recordLen,
                0, fileSize, recordLen);
    }

    @Test
    public void test_stream_two_files_fill_first_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects - 1) ;
        int expectedFileLength = HeaderOps.HEADER_SIZE + recordLen;
        test_stream(testContext, numObjects, fileSize, recordLen - 1, recordLen,
                1, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_two_files_fill_first_with_empty_space_small_rb(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects - 1) + (recordLen / 2) ;
        int expectedFileLength = HeaderOps.HEADER_SIZE + recordLen;
        test_stream(testContext, numObjects, fileSize, recordLen - 1, recordLen,
                1, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_two_files_fill_both_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects / 2 ;
        test_stream(testContext, numObjects, fileSize, recordLen - 1, recordLen,
                1, fileSize, recordLen);
    }

    @Test
    public void test_stream_five_files_with_empty_space_small_rb(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects / 5) + (recordLen / 2) ;
        int expectedFileLength = HeaderOps.HEADER_SIZE + recordLen * (numObjects / 5);
        test_stream(testContext, numObjects, fileSize, recordLen - 1, recordLen,
                4, expectedFileLength, recordLen);
    }

    @Test
    public void test_stream_five_files_fill_all_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects / 5 ;
        test_stream(testContext, numObjects, fileSize, recordLen - 1, recordLen,
                4, fileSize , recordLen);
    }

    protected void test_stream(TestContext testContext, int numObjects, int maxLogChunkSize, int readBuffersize,
                               int maxRecordSize, int expectedEndFile, int expectedEndFileLength, int objLen) throws Exception {
        test_stream(testContext, numObjects, numObjects, maxLogChunkSize, readBuffersize, maxRecordSize, expectedEndFile, expectedEndFileLength,
                objLen, 0);
    }

    protected void test_stream(TestContext testContext, int numAppendObjects, int numReadObjects, int maxLogChunkSize, int readBuffersize,
                               int maxRecordSize, int expectedEndFile, int expectedEndFileLength, int objLen,
                               long startEventNum)
            throws Exception {
        serverOptions = origServerOptions().
                        setMaxLogChunkSize(maxLogChunkSize).
                        setReadBufferSize(readBuffersize).
                        setMaxRecordSize(maxRecordSize);
        startLog();

        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        publishObjectsSequentially(numAppendObjects, i -> obj.copy().put("num", i));

        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        int offset = numAppendObjects - numReadObjects;

        LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(startEventNum));
        rs.handler((recordNum, record) -> {
            // event is good
            BsonObject event = record.getBsonObject("event");
            testContext.assertEquals("bar", event.getString("foo"));
            // is record number agreed
            long expectedRecordNum = cnt.get() + offset;
            testContext.assertEquals(expectedRecordNum, (long)event.getInteger("num"));
            testContext.assertEquals(expectedRecordNum, recordNum);

            if (cnt.incrementAndGet() == numReadObjects) {
                rs.close();
                testContext.assertEquals(expectedEndFile, log.getFileNumber());
                // Check the lengths of the files
                File[] files = super.listLogFiles(logsDir, TEST_CHANNEL_1);
                String headFileName = getLogFileName(TEST_CHANNEL_1, log.getFileNumber());
                String preallocedFileName = getLogFileName(TEST_CHANNEL_1, log.getFileNumber() + 1);
                for (File f : files) {
                    String fname = f.getName();
                    if (fname.equals(headFileName)) {
                        testContext.assertEquals((long)expectedEndFileLength, f.length());
                    } else if (!fname.equals(preallocedFileName)) {
                        testContext.assertEquals((long)maxLogChunkSize, f.length());
                    }
                }
                async.complete();
            }
        });
        rs.start();
    }

    @Test
    //@Repeat(value = 1000)
    public void test_pause_resume_in_retro(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * 20;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(recordLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(0));
        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        AtomicBoolean paused = new AtomicBoolean();
        rs.handler((recordNum, record) -> {
            // should not deliver when paused
            testContext.assertFalse(paused.get());

            // event is good
            BsonObject event = record.getBsonObject("event");
            testContext.assertEquals("bar", event.getString("foo"));
            // is record number agreed
            testContext.assertEquals(cnt.get(), event.getInteger("num"));
            testContext.assertEquals((long)cnt.get(), recordNum);

            if (cnt.incrementAndGet() == numObjects) {
                rs.close();
                async.complete();
            }
            // Pause every 5 msgs
            if (cnt.get() % 5 == 0) {
                rs.pause();
                paused.set(true);
                vertx.setTimer(10, tid -> {
                    paused.set(false);
                    rs.resume();
                });
            }
        });
        rs.start();
    }

    @Test
    public void test_stream_from_non_zero_position(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects / 5 + recordLen / 2;
        int startEvent = 50;
        int expectedFileLength = HeaderOps.HEADER_SIZE + recordLen * numObjects / 5;
                test_stream(testContext, 100, 50, fileSize, recordLen - 1, recordLen,
                4, expectedFileLength , recordLen, startEvent);
    }

    @Test
    public void test_stream_from_negative_position(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * 20;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(recordLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        try {
            LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(-2));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_stream_from_past_head(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * 20;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(recordLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        try {
            LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(((LogImpl)log).getLastWrittenSeq() + 1));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    //@Repeat(value = 10000)
    public void test_stream_from_last_written(TestContext testContext) throws Exception {

        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects + 10;
        serverOptions = origServerOptions().
                        setMaxLogChunkSize(fileSize).
                        setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).
                        setMaxRecordSize(recordLen);

        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        long lastWritten = ((LogImpl)log).getLastWrittenSeq();
        LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(lastWritten));

        Async async1 = testContext.async();
        Async async2 = testContext.async();
        AtomicInteger cnt = new AtomicInteger(numObjects - 1);
        rs.handler((recordNum, record) -> {
            // event is good
            BsonObject event = record.getBsonObject("event");
            testContext.assertEquals("bar", event.getString("foo"));
            // is record number agreed
            testContext.assertEquals(cnt.get(), event.getInteger("num"));
            testContext.assertEquals((long)cnt.get(), recordNum);
            testContext.assertEquals("bar", event.getString("foo"));

            int currCount = cnt.get();
            if (currCount == numObjects - 1) {
                async1.complete();
            }

            if (cnt.incrementAndGet() == numObjects * 2) {
                rs.close();
                async2.complete();
            }
        });
        rs.start();

        // Append some more after the head has been consumed - log will then switch into live mode

        async1.await();

        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i + numObjects));
    }

    @Test
    public void test_stream_active_from_zero(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects + 10;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(recordLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(-1));

        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        rs.handler((recordNum, record) -> {
            // event is good
            BsonObject event = record.getBsonObject("event");
            testContext.assertEquals("bar", event.getString("foo"));
            // is record number agreed
            testContext.assertEquals(cnt.get(), event.getInteger("num"));
            testContext.assertEquals((long)cnt.get(), recordNum);

            testContext.assertFalse(rs.isRetro());
            if (cnt.incrementAndGet() == numObjects) {
                rs.close();
                async.complete();
            }
        });
        rs.start();

        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
    }

    @Test
    //@Repeat(value=10000)
    public void test_pause_resume_active_retro_active(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects + 10;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(recordLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(-1));

        Async async1 = testContext.async();
        Async async2 = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        testContext.assertFalse(rs.isRetro());
        rs.handler((recordNum, record) -> {
            // event is good
            BsonObject event = record.getBsonObject("event");
            testContext.assertEquals("bar", event.getString("foo"));
            // is record number agreed
            testContext.assertEquals(cnt.get(), event.getInteger("num"));
            testContext.assertEquals((long)cnt.get(), recordNum);


            int currCount = cnt.get();
            if (currCount == numObjects / 2 - 1) {
                // When received half the messages pause then resume after a few ms,
                //log will then be in retro mode
                testContext.assertFalse(rs.isRetro());
                rs.pause();
                vertx.setTimer(10, tid -> {
                    rs.resume();
                    testContext.assertTrue(rs.isRetro());
                });
            }

            if (cnt.incrementAndGet() == numObjects) {
                async1.complete();
            }
            if (cnt.get() == numObjects * 2) {
                rs.close();
                async2.complete();
            }
        });
        rs.start();

        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        async1.await();

        // Now send some more messages - sub should be active again

        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i + numObjects));
    }

    @Test
    public void test_pause_resume_active_active(TestContext testContext) throws Exception {
        int fileSize = HeaderOps.HEADER_SIZE + recordLen * numObjects + 10;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(recordLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(-1));

        testContext.assertFalse(rs.isRetro());

        Async async1 = testContext.async();
        Async async2 = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        rs.handler((recordNum, record) -> {
            BsonObject event = record.getBsonObject("event");
            // event is good ??
            testContext.assertEquals("bar", event.getString("foo"));
            // All agreed on the object number
            int currCount = cnt.get();
            testContext.assertEquals((long)currCount, recordNum);
            testContext.assertEquals(currCount, event.getInteger("num"));


            testContext.assertFalse(rs.isRetro());
            if (cnt.incrementAndGet() == numObjects) {
                // Pause then resume. Don't publish any more messages when paused so consumer stays
                // active
                rs.pause();
                vertx.setTimer(10, tid -> {
                    rs.resume();
                    testContext.assertFalse(rs.isRetro());
                    async1.complete();
                });

            }
            if (cnt.get() == numObjects * 2) {
                testContext.assertFalse(rs.isRetro());
                rs.close();
                async2.complete();
            }
        });
        rs.start();

        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        async1.await();

        // Now send some more messages - should stay active

        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i + numObjects));

    }




    @Test
    //@Repeat(value = 10000)
    public void test_stream_multiple(TestContext testContext) throws Exception {

        int fileSize = HeaderOps.HEADER_SIZE + recordLen * (numObjects / 5) + recordLen / 2 + (FramingOps.FRAME_SIZE + Integer.BYTES);
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(recordLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        publishObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        LogReadStreamImpl rs1 = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(0));
        LogReadStreamImpl rs2 = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartEventNum(0));


        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger counter1 = new AtomicInteger();
        AtomicInteger counter2 = new AtomicInteger();

        handleRecords(rs1, counter1, testContext, latch);
        handleRecords(rs2, counter2, testContext, latch);

        latch.await();
    }


    private void handleRecords(LogReadStreamImpl rs, AtomicInteger counter, TestContext testContext, CountDownLatch latch) {

        rs.handler((recordNum, record) -> {
            // event is good
            BsonObject event = record.getBsonObject("event");
            testContext.assertEquals("bar", event.getString("foo"));
            // is record number agreed
            testContext.assertEquals(counter.get(), event.getInteger("num"));
            testContext.assertEquals((long)counter.get(), recordNum);

            if (counter.incrementAndGet() == numObjects) {
                rs.close();
                latch.countDown();
            }
        });
        rs.start();
    }


}

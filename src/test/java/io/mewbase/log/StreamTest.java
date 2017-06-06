package io.mewbase.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.LogReadStream;
import io.mewbase.server.ServerOptions;
import io.mewbase.server.impl.log.FramingOps;
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

    private BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
    private int objLen = obj.encode().length() + FramingOps.FRAME_SIZE;
    private int numObjects = 100;

    @Test
    public void test_stream_single_file_less_than_max_file_size(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects + 10);
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_single_file_equal_to_max_file_size(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_exactly(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1) + FramingOps.FRAME_SIZE + Integer.BYTES;;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_with_empty_space(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1) + (objLen / 2) + FramingOps.FRAME_SIZE + Integer.BYTES; ;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_both_exactly(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects / 2 + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                1, fileSize - (FramingOps.FRAME_SIZE + Integer.BYTES) , objLen);
    }

    @Test
    public void test_stream_five_files_with_empty_space(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects / 5) + (objLen / 2) + FramingOps.FRAME_SIZE + Integer.BYTES;;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                4, objLen * (numObjects / 5), objLen);
    }

    @Test
    public void test_stream_five_files_fill_both_exactly(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects / 5 + FramingOps.FRAME_SIZE + Integer.BYTES;;
        test_stream(testContext, numObjects, fileSize, ServerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                4, fileSize - (FramingOps.FRAME_SIZE + Integer.BYTES), objLen);
    }

    @Test
    public void test_stream_single_file_less_than_max_file_size_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects + 10) + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_single_file_equal_to_max_file_size_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1) + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_with_empty_space_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1) + (objLen / 2) + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_both_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects / 2 + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                1, fileSize - (FramingOps.FRAME_SIZE + Integer.BYTES), objLen);
    }

    @Test
    public void test_stream_five_files_with_empty_space_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects / 5) + (objLen / 2) + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                4, objLen * (numObjects / 5), objLen);
    }

    @Test
    public void test_stream_five_files_fill_both_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects / 5 + FramingOps.FRAME_SIZE + Integer.BYTES;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                4, fileSize - (FramingOps.FRAME_SIZE + Integer.BYTES), objLen);
    }

    protected void test_stream(TestContext testContext, int numObjects, int maxLogChunkSize, int readBuffersize,
                               int maxRecordSize, int expectedEndFile, int expectedEndFileLength, int objLen) throws Exception {
        test_stream(testContext, numObjects, numObjects, maxLogChunkSize, readBuffersize, maxRecordSize, expectedEndFile, expectedEndFileLength,
                objLen, 0);
    }

    protected void test_stream(TestContext testContext, int numAppendObjects, int numReadObjects, int maxLogChunkSize, int readBuffersize,
                               int maxRecordSize, int expectedEndFile, int expectedEndFileLength, int objLen,
                               long startPos)
            throws Exception {
        serverOptions = origServerOptions().setMaxLogChunkSize(maxLogChunkSize).
                setReadBufferSize(readBuffersize).setMaxRecordSize(maxRecordSize);
        startLog();

        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        appendObjectsSequentially(numAppendObjects, i -> obj.copy().put("num", i));

        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        int offset = numAppendObjects - numReadObjects;
        LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(startPos));
        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
            testContext.assertEquals(cnt.get() + offset, record.getInteger("num"));
            long expectedPos = calcPos(cnt.get() + offset, maxLogChunkSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
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
        int fileSize = objLen * 20;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(0));
        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        AtomicBoolean paused = new AtomicBoolean();
        rs.handler((pos, record) -> {
            testContext.assertFalse(paused.get());
            testContext.assertEquals("bar", record.getString("foo"));
            testContext.assertEquals(cnt.get(), record.getInteger("num"));
            long expectedPos = calcPos(cnt.get(), fileSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
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
        int fileSize = objLen * numObjects / 5 + objLen / 2;
        long startPos = calcPos(50, fileSize, objLen);
        test_stream(testContext, 100, 50, fileSize, objLen - 1, objLen,
                4, objLen * numObjects / 5, objLen, startPos);
    }

    @Test
    public void test_stream_from_negative_position(TestContext testContext) throws Exception {
        int fileSize = objLen * 20;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        try {
            LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(-2));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_stream_from_past_head(TestContext testContext) throws Exception {
        int fileSize = objLen * 20;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        try {
            LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(((LogImpl)log).getLastWrittenPos() + 1));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    //@Repeat(value = 10000)
    public void test_stream_from_last_written(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects + 10;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        LogReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(((LogImpl)log).getLastWrittenPos()));

        Async async1 = testContext.async();
        Async async2 = testContext.async();
        AtomicInteger cnt = new AtomicInteger(numObjects - 1);
        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
            int currCount = cnt.get();
            if (currCount == numObjects - 1) {
                async1.complete();
            }
            testContext.assertEquals(currCount, record.getInteger("num"));
            long expectedPos = calcPos(currCount, fileSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
            if (cnt.incrementAndGet() == numObjects * 2) {
                rs.close();
                async2.complete();
            }
        });
        rs.start();

        // Append some more after the head has been consumed - log will then switch into live mode

        async1.await();

        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i + numObjects));
    }

    @Test
    public void test_stream_active_from_zero(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects + 10;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(-1));

        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
            int currCount = cnt.get();
            testContext.assertEquals(currCount, record.getInteger("num"));
            long expectedPos = calcPos(currCount, fileSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
            testContext.assertFalse(rs.isRetro());
            if (cnt.incrementAndGet() == numObjects) {
                rs.close();
                async.complete();
            }
        });
        rs.start();

        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
    }

    @Test
    //@Repeat(value=10000)
    public void test_pause_resume_active_retro_active(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects + 10;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(-1));

        Async async1 = testContext.async();
        Async async2 = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        testContext.assertFalse(rs.isRetro());
        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
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
            testContext.assertEquals(currCount, record.getInteger("num"));
            long expectedPos = calcPos(currCount, fileSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
            if (cnt.incrementAndGet() == numObjects) {
                async1.complete();
            }
            if (cnt.get() == numObjects * 2) {
                rs.close();
                async2.complete();
            }
        });
        rs.start();

        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        async1.await();

        // Now send some more messages - sub should be active again

        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i + numObjects));
    }

    @Test
    public void test_pause_resume_active_active(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects + 10;
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        LogReadStreamImpl rs = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(-1));

        testContext.assertFalse(rs.isRetro());

        Async async1 = testContext.async();
        Async async2 = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
            int currCount = cnt.get();

            testContext.assertEquals(currCount, record.getInteger("num"));
            long expectedPos = calcPos(currCount, fileSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
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

        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        async1.await();

        // Now send some more messages - should stay active

        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i + numObjects));

    }


    /*
    Calculate the append position of the nth object to be appended to the log, n starts at zero
     */
    protected long calcPos(int nth, int maxLogChunkSize, int objLength) {

        int pos = 0;
        int filePos = 0;

        for (int i = 0; i < nth; i++) {
            pos += objLength;
            filePos += objLength;
            int remainingSpace = maxLogChunkSize - filePos;
            if (remainingSpace < objLength + FramingOps.FRAME_SIZE + Integer.BYTES) {
                pos += remainingSpace;
                filePos = 0;
            }
        }

        return pos;
    }

    @Test
    //@Repeat(value = 10000)
    public void test_stream_multiple(TestContext testContext) throws Exception {

        int fileSize = objLen * (numObjects / 5) + objLen / 2 + (FramingOps.FRAME_SIZE + Integer.BYTES);
        serverOptions = origServerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(ServerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen);
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);

        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        LogReadStreamImpl rs1 = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(0));
        LogReadStreamImpl rs2 = (LogReadStreamImpl)log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1).setStartPos(0));


        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger counter1 = new AtomicInteger();
        AtomicInteger counter2 = new AtomicInteger();

        handleRecords(rs1, counter1, testContext, latch, fileSize);
        handleRecords(rs2, counter2, testContext, latch, fileSize);

        latch.await();
    }

    private void handleRecords(LogReadStreamImpl rs, AtomicInteger counter, TestContext testContext, CountDownLatch latch, int fileSize) {

        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
            // int currCount = counter.get();
            testContext.assertEquals(counter.get(), record.getInteger("num"));
            long expectedPos = calcPos(counter.get(), fileSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
            if (counter.incrementAndGet() == numObjects) {
                rs.close();
                latch.countDown();
            }
        });
        rs.start();
    }


}

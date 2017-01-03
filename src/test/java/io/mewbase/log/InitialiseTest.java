package io.mewbase.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.Log;
import io.mewbase.server.impl.ServerImpl;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

import static io.mewbase.server.ServerOptions.DEFAULT_MAX_LOG_CHUNK_SIZE;
import static org.junit.Assert.*;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class InitialiseTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(InitialiseTest.class);

    @Test
    public void test_when_starting_log_dir_is_created() throws Exception {
        File ftestDir = testFolder.newFolder();
        String subDir = UUID.randomUUID().toString();
        logsDir = new File(ftestDir, subDir);
        assertFalse(logsDir.exists());
        serverOptions = origServerOptions().setLogsDir(logsDir.getPath());
        startLog();
        assertTrue(logsDir.exists());
    }

    @Test
    public void test_when_starting_with_existing_log_dir_new_files_are_created() throws Exception {
        startLog();
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
    }

    @Test
    public void test_when_starting_and_non_zero_preallocate_size() throws Exception {
        serverOptions = origServerOptions().setPreallocateSize(1024 * 1024);
        startLog();
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
    }

    @Test
    public void test_when_starting_without_existing_log_dir_new_files_are_created() throws Exception {
        File ftestDir = testFolder.newFolder();
        String subDir = UUID.randomUUID().toString();
        File ld = new File(ftestDir, subDir);
        assertFalse(ld.exists());
        logsDir = ld;
        serverOptions = origServerOptions().setLogsDir(ld.getPath());
        startLog();
        verifyInitialFiles(ld, TEST_CHANNEL_1);
    }

    @Test
    public void test_when_restarting_existing_files_are_unchanged() throws Exception {
        startLog();
        // Create the files
        test_when_starting_with_existing_log_dir_new_files_are_created();
        log.close().get();
        log.start().get();
        test_when_starting_with_existing_log_dir_new_files_are_created();
    }

    @Test
    public void test_when_starting_log_other_channel_files_are_unchanged() throws Exception {
        startLog();
        server.createChannel(TEST_CHANNEL_2).get();
        Log log2 = ((ServerImpl)server).getLog(TEST_CHANNEL_2);
        log2.start().get();
        verifyInitialFiles(logsDir, TEST_CHANNEL_2);
    }

    @Test
    public void test_start_with_zeroed_info_file_but_no_log_file() throws Exception {
        startLog();
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
        log.close().get();

        File logFile = new File(logsDir, TEST_CHANNEL_1 + "-0.log");
        assertTrue(logFile.exists());

        assertTrue(logFile.delete());
        assertFalse(logFile.exists());

        // Start again should succeed as the info file contains just zeros
        startLog();
    }

    @Test
    public void test_start_with_non_zero_file_pos_but_no_log_file() throws Exception {
        startLog();
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
        log.close().get();

        File logFile = new File(logsDir, TEST_CHANNEL_1 + "-0.log");
        assertTrue(logFile.exists());

        assertTrue(logFile.delete());
        assertFalse(logFile.exists());

        // Now change info file to non zero fileHeadPos
        saveInfo(0, 23, 23, 0, false);

        // Start should now fail
        try {
            startLog();
            fail("Should throw exception");
        } catch (Exception e) {
            // OK
        }
        log = null;
    }

    @Test
    public void test_start_with_non_zero_file_number_but_no_log_file() throws Exception {
        startLog();
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
        log.close().get();

        File logFile = new File(logsDir, TEST_CHANNEL_1 + "-0.log");
        assertTrue(logFile.exists());

        assertTrue(logFile.delete());
        assertFalse(logFile.exists());

        // Now change info file to non zero fileHeadPos
        saveInfo(1, 0, 0, 0, false);

        // Start should now fail
        try {
            startLog();
            fail("Should throw exception");
        } catch (Exception e) {
            // OK
        }
        log = null;
    }

    @Test
    public void test_start_with_negative_file_number() throws Exception {
        test_start_with_invalid_info_file(() -> {
            saveInfo(-1, 0, 0, 0, false);
        });
    }

    @Test
    public void test_start_with_negative_file_head_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            saveInfo(0, -1, 0, 0, false);
        });
    }

    @Test
    public void test_start_with_negative_head_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            saveInfo(0, 0, -1, 0, false);
        });
    }

    @Test
    public void test_start_with_negative_last_written_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            saveInfo(0, 0, 0, -1, false);
        });
    }


    @Test
    public void test_start_with_missing_file_number() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            info.put("lastWrittenPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_missing_file_head_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("lastWrittenPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_missing_last_written_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_missing_head_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("fileHeadPos", 0);
            info.put("lastWrittenPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_missing_shutdown() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            info.put("lastWrittenPos", 0);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_file_number() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", "XYZ");
            info.put("headPos", 0);
            info.put("lastWrittenPos", 0);
            info.put("fileHeadPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_file_head_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("lastWrittenPos", 0);
            info.put("fileHeadPos", "XYZ");
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_last_written_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("lastWrittenPos", "XYZ");
            info.put("fileHeadPos", "XYZ");
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_head_pos() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", "XYZ");
            info.put("fileHeadPos", 0);
            info.put("lastWrittenPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_shutdown() throws Exception {
        test_start_with_invalid_info_file(() -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            info.put("lastWrittenPos", 0);
            info.put("shutdown", "XYZ");
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_max_record_size_too_large() throws Exception {
        serverOptions = origServerOptions().setMaxRecordSize(DEFAULT_MAX_LOG_CHUNK_SIZE + 1);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_start_preallocate_size_too_large() throws Exception {
        serverOptions = origServerOptions().setPreallocateSize(DEFAULT_MAX_LOG_CHUNK_SIZE + 1);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_start_max_record_size_too_small() throws Exception {
        serverOptions = origServerOptions().setMaxRecordSize(0);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_max_log_chunk_size_too_small() throws Exception {
        serverOptions = origServerOptions().setMaxLogChunkSize(0);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_start_negative_preallocate_size() throws Exception {
        serverOptions = origServerOptions().setPreallocateSize(-1);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }


    private void verifyInitialFiles(File logDir, String channel) throws Exception {
        verifyInfoFile(channel);

        File logFile = new File(logDir, channel + "-0.log");
        assertTrue(logFile.exists());
        assertEquals(serverOptions.getPreallocateSize(), logFile.length());

        String[] dirList = logDir.list((dir, name) -> name.endsWith(".log") && name.startsWith(channel));
        assertNotNull(dirList);
        assertEquals(1, dirList.length);
        assertEquals(logFile.getName(), dirList[0]);
    }

    private void verifyInfoFile(String channel) {
        File infoFile = new File(logsDir, channel + "-log-info.dat");
        assertTrue(infoFile.exists());
        BsonObject info = readInfoFromFile(infoFile);
        Integer fileNumber = info.getInteger("fileNumber");
        assertNotNull(fileNumber);
        assertEquals(0, (long)fileNumber);
        Integer fileHeadPos = info.getInteger("fileHeadPos");
        assertNotNull(fileHeadPos);
        assertEquals(0, (long)fileHeadPos);
        Integer headPos = info.getInteger("headPos");
        assertNotNull(headPos);
        assertEquals(0, (long)headPos);
        Boolean shutdown = info.getBoolean("shutdown");
        assertNotNull(shutdown);
        assertTrue(shutdown);
    }


    private void test_start_with_invalid_info_file(Runnable runner) throws Exception {
        startLog();
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
        log.close().get();

        // Now change info file
        runner.run();

        // Start should now fail
        try {
            startLog();
            fail("Should throw exception");
        } catch (Exception e) {
            // OK
        }
        log = null;
    }


}

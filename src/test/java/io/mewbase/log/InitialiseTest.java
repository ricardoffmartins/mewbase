package io.mewbase.log;

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

    private final static String INITIAL_FILE_END =  "-000000000000.log";


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
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
        stopServerAndClient();
        startLog();
        verifyInitialFiles(logsDir, TEST_CHANNEL_1);
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

    private void verifyInitialFiles(File logDir, String channel, boolean shutdown) throws Exception {

        File logFile = new File(logDir, channel + INITIAL_FILE_END);
        assertTrue(logFile.exists());
        assertEquals(serverOptions.getPreallocateSize(), logFile.length());

        String[] dirList = logDir.list((dir, name) -> name.endsWith(".log") && name.startsWith(channel));
        assertNotNull(dirList);
        assertEquals(1, dirList.length);
        assertEquals(logFile.getName(), dirList[0]);
    }

    private void verifyInitialFiles(File logDir, String channel) throws Exception {
        verifyInitialFiles(logDir, channel, false);
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

package io.mewbase.log;


import io.mewbase.client.MewException;
import io.mewbase.server.impl.log.HeaderOps;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


/**
 * Created by Nige on 01/06/17.
 */
@RunWith(VertxUnitRunner.class)
public class HeaderOpsTest extends LogTestBase {

    // Patching this in to catch the log output
    class TestAppender extends AppenderSkeleton {
        private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        protected void append(final LoggingEvent loggingEvent) {
            log.add(loggingEvent);
        }

        @Override
        public void close() {
        }

        public List<LoggingEvent> getLog() {
            return new ArrayList<LoggingEvent>(log);
        }
    }


    private TestAppender patchTestAppender() {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        return appender;
    }

    private void removeTestAppender(TestAppender appender) {
        final Logger logger = Logger.getRootLogger();
        logger.removeAppender(appender);
    }


    @Test
    public void testReversable() throws Exception {
        final HeaderOps hops = new HeaderOps();
        final long expectedRecordNum = 1234567890l;
        final long expectedTimestamp = System.currentTimeMillis();
        Buffer header = hops.makeHeader(expectedRecordNum,expectedTimestamp);
        HeaderOps.HeaderDetails dets = hops.readHeader(header);
        assertEquals(expectedRecordNum, dets.getRecordNumber());
        assertEquals(expectedTimestamp, dets.getTimestamp());
    }


    @Test
    public void testBadVersion() throws Exception {

        final HeaderOps hops = new HeaderOps();
        final long expectedRecordNum = 1234567890l;
        final long expectedTimestamp = System.currentTimeMillis();
        Buffer header = hops.makeHeader(expectedRecordNum,expectedTimestamp);
        try {
            Buffer withBadVerison = header.setByte(16,  (byte)128);
            HeaderOps.HeaderDetails dets = hops.readHeader(withBadVerison);
            fail("Failed to throw exception for bad version header");
        } catch (MewException exp) {
            assertTrue(exp.getMessage().contains("version"));
        }
    }


    @Test
    public void testCorruptFileHeader() throws Exception {

        final TestAppender appender = patchTestAppender();

        final HeaderOps hops = new HeaderOps();
        final long expectedRecordNum = 1234567890l;
        final long expectedTimestamp = System.currentTimeMillis();
        Buffer header = hops.makeHeader(expectedRecordNum, expectedTimestamp);
        Buffer withCorruptHeader = header.setByte(18,  (byte)23);
        HeaderOps.HeaderDetails dets = hops.readHeader(withCorruptHeader);

        final LoggingEvent logEntry = appender.getLog().get(0);
        assertThat(logEntry.getLevel(), is(Level.ERROR));
        assertTrue(logEntry.getMessage().toString().contains("corrupt"));
        removeTestAppender(appender);
    }


}

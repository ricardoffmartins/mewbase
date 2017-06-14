package io.mewbase.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.MewException;
import io.mewbase.server.impl.log.FramingOps;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.log4j.Logger;



import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by Nige on 01/06/17.
 */
@RunWith(VertxUnitRunner.class)
public class FramingOpsTest extends LogTestBase {

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
        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withFrame = framing.frame(original);
        Buffer result = framing.unframe(withFrame);
        assertEquals(original, result);
    }


    @Test
    public void testBadChecksum() throws Exception {

        final TestAppender appender = patchTestAppender();

        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withBadChecksum = framing.frame(original).setByte( 2, (byte) 43);

        // read with bad checksum
        Buffer result = framing.unframe(withBadChecksum);

        final LoggingEvent logEntry = appender.getLog().get(0);
        assertThat(logEntry.getLevel(), is(Level.ERROR));
        assertTrue(logEntry.getMessage().toString().contains("Checksum"));
        removeTestAppender(appender);
    }


    @Test
    public void testCorruptMessageSize() throws Exception {

        final TestAppender appender = patchTestAppender();

        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withCorruptMessageSize = framing.frame(original).setByte(4,  (byte)0);

        Buffer result = framing.unframe(withCorruptMessageSize);

        final LoggingEvent logEntry = appender.getLog().get(0);
        assertThat(logEntry.getLevel(), is(Level.ERROR));
        assertTrue(logEntry.getMessage().toString().contains("Checksum"));
        removeTestAppender(appender);
    }

    @Test
    public void testCorruptMessage() throws Exception {

        final TestAppender appender = patchTestAppender();

        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withCorruptMessage = framing.frame(original).setByte(12,  (byte)42);

        Buffer result = framing.unframe(withCorruptMessage);

        final LoggingEvent logEntry = appender.getLog().get(0);
        assertThat(logEntry.getLevel(), is(Level.ERROR));
        assertTrue(logEntry.getMessage().toString().contains("Checksum"));
        removeTestAppender(appender);
    }

    @Test
    public void testBadMagic() throws Exception {

        final TestAppender appender = patchTestAppender();

        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        final int offsetIntoMagic = FramingOps.CHECKSUM_SIZE + original.length() + 7;
        Buffer framed = framing.frame(original).copy();
        Buffer withCorruptMagic = framed.setByte(offsetIntoMagic,  (byte)6);

        Buffer result = framing.unframe(withCorruptMagic);

        final LoggingEvent logEntry = appender.getLog().get(0);
        assertThat(logEntry.getLevel(), is(Level.ERROR));
        assertTrue(logEntry.getMessage().toString().contains("Magic"));
        removeTestAppender(appender);

    }


}

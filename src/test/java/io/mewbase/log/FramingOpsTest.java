package io.mewbase.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.MewException;
import io.mewbase.server.impl.log.FramingOps;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

/**
 * Created by Nige on 01/06/17.
 */
@RunWith(VertxUnitRunner.class)
public class FramingOpsTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(FramingOpsTest.class);

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
        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withBadChecksum = framing.frame(original).setByte( 2, (byte) 43);
        try {
            Buffer result = framing.unframe(withBadChecksum);
            fail("Good read from bad checksum");
        } catch (MewException mewException) {
            assertTrue( mewException.getErrorCode() == FramingOps.CHECKSUM_ERROR );
        }
    }

    @Test
    public void testCorruptMessageSize() throws Exception {
        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withCorruptMessageSize = framing.frame(original).setByte(4,  (byte)0);
        try {
            Buffer result = framing.unframe(withCorruptMessageSize);
            fail("Good read from corrupt message");
        } catch (MewException mewException) {
            assertTrue( mewException.getErrorCode() == FramingOps.CHECKSUM_ERROR );
        }
    }

    @Test
    public void testCorruptMessage() throws Exception {
        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withCorruptMessage = framing.frame(original).setByte(12,  (byte)42);
        try {
            Buffer result = framing.unframe(withCorruptMessage);
            fail("Good read from corrupt message");
        } catch (MewException mewException) {
            assertTrue(mewException.getErrorCode() == FramingOps.CHECKSUM_ERROR );
        }
    }

    @Test
    public void testBadMagic() throws Exception {
        final FramingOps framing = new FramingOps();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        final int offsetIntoMagic = FramingOps.CHECKSUM_SIZE + original.length() + 7;
        Buffer framed = framing.frame(original).copy();
        Buffer withCorruptMagic = framed.setByte(offsetIntoMagic,  (byte)6);
        try {
            Buffer result = framing.unframe(withCorruptMagic);
            fail("Good read from corrupt message");
        } catch (MewException mewException) {
            assertTrue(mewException.getErrorCode() == FramingOps.MAGIC_BYTES_ERROR );
        }
    }


}

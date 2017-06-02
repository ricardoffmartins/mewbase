package io.mewbase.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.impl.log.HeaderOps;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by Nige on 01/06/17.
 */
@RunWith(VertxUnitRunner.class)
public class HeaderOpsTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(HeaderOpsTest.class);

    @Test
    public void testReversable() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withChecksum = HeaderOps.writeHeader(original);
        Buffer result = HeaderOps.readHeader(withChecksum);
        assertEquals(original, result);
    }

    @Test
    public void testCorruptMagic() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withCorruptMagic = HeaderOps.writeHeader(original).setByte(0, (byte) 42);
        try {
            Buffer result = HeaderOps.readHeader(withCorruptMagic);
            fail("Good read from corrupt magic number");
        } catch (Exception mewException) {
            assertTrue( mewException.getMessage().contains("Magic") );
        }
    }


    @Test
    public void testBadCodec() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withBadCodec = HeaderOps.writeHeader(original).setByte(1, (byte) 42);
        try {
            Buffer result = HeaderOps.readHeader(withBadCodec);
            fail("Good read from bad codec number");
        } catch (Exception mewException) {
            assertTrue( mewException.getMessage().contains("Codec") );
        }
    }

    @Test
    public void testBadChecksum() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withBadChecksum = HeaderOps.writeHeader(original).setByte(4, (byte) 42);
        try {
            Buffer result = HeaderOps.readHeader(withBadChecksum);
            fail("Good read from bad checksum");
        } catch (Exception mewException) {
            assertTrue( mewException.getMessage().contains("Checksum") );
        }
    }


    @Test
    public void testCorruptMessage() throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 23456);
        Buffer original = obj.encode();
        Buffer withCorruptMessage = HeaderOps.writeHeader(original).setByte(12,  (byte)42);
        try {
            Buffer result = HeaderOps.readHeader(withCorruptMessage);
            fail("Good read from corrupt message");
        } catch (Exception mewException) {
            assertTrue(mewException.getMessage().contains("Checksum"));
        }
    }

}

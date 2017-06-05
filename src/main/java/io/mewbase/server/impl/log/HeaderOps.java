package io.mewbase.server.impl.log;


import io.mewbase.client.MewException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.buffer.Buffer;

import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * The HeaderOps intended to manipulate Buffers (Vertx over Netty) in order to add/remove a header
 * from each Bson encoded event (record) that is written into the log
 *
 * | Magic - 8 bits            | Immediate check something is missaligned
 * | CompressionCodec - 8 bits | 0 is no compression (allows dynamic compression for large records)
 * | Checksum - 32 bits        | checksum of body
 * | Body - Variable           | Body of single event
 *
 * Magic Byte is the least occurring byte (octet) in some large binary and log files using ...
 *
 *  $ hexdump -v -e '"x" 1/1 "%02X" "\n"' <<filepath>>  | sort -S 80% | uniq -c | sort -bnr
 *
 *  We should run this over some 'production' log files later to check for anything that occour significantly less.
 *
 */
public class HeaderOps {

    static final byte MAGIC_BYTE = 0x40;   // see above notes

    static final byte NO_COMPRESSION_CODEC = 0x00;

    // header layout
    static final int MAGIC_OFFSET = 0;
    static final int CODEC_OFFSET = Byte.BYTES;
    static final int CHECKSUM_OFFSET = CODEC_OFFSET + Byte.BYTES;
    public static final int HEADER_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;

    //static final Checksum checksumOp = new CRC32();
    static final Checksum checksumOp = new Adler32();


    /**
     * Wrap the stateful checksum op in a pure function
     */
    private static synchronized int getChecksum(byte[]  bytes) {
        checksumOp.reset();
        checksumOp.update(bytes, 0 , bytes.length);
        return (int)checksumOp.getValue();
    }


    /**
     * Given a Vertx Buffer of bytes - add a header in the above format
     * using a netty zero copy compound buffer
     * @param in : Vertx Buffer of arbitrary bytes
     * @return Buffer with checksum in the header
     */
    public static Buffer writeHeader(Buffer in)  {
            final byte[] inBytes = in.getBytes();
            final ByteBuf header = Unpooled.buffer(HEADER_OFFSET);
            header.writeByte(MAGIC_BYTE);
            // expand this to add dynamic compression
            header.writeByte(NO_COMPRESSION_CODEC);
            final int checksum = getChecksum(inBytes);
            header.writeInt(checksum);
            // zero copy compound buffer wrapped by Vertx Buffer
            return  Buffer.buffer(Unpooled.wrappedBuffer(header, in.getByteBuf()));
    }

    /**
     * Read a buffer with an encoded header and perform various checks as the header is removed
     * @param in : Vertx Buffer with a header as described above.
     * @return : A buffer without the  header
     */
    public static Buffer readHeader(Buffer in) {

        final byte headerMagic = in.getByte(MAGIC_OFFSET);
        if (headerMagic != MAGIC_BYTE) {
            throw new MewException("Magic byte error - suspect misaligned record");
        }

        // expand this to add dynamic compression
        final byte headerCodec = in.getByte(CODEC_OFFSET);
        if (headerCodec != NO_COMPRESSION_CODEC) {
            throw new MewException("Compression Codec error - unknown compression codec number");
        }

        final int headerChecksum = in.getInt(CHECKSUM_OFFSET);
        final byte[] msgBytes = in.getBytes(HEADER_OFFSET, in.length());
        final int checksum = getChecksum(msgBytes);
        if (headerChecksum != checksum) {
            throw new MewException("Checksum error - probable message corruption");
        }
        return Buffer.buffer(msgBytes);
    }


}

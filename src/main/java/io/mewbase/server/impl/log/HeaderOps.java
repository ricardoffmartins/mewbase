package io.mewbase.server.impl.log;


import io.mewbase.client.MewException;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


/**
 * The HeaderOps intended to write and read a header from an Async log file
 *
 * | Record Number - 64 bits  | The number of the first record written to this file
 * | Timestamp     - 64 bits  | Timestamp (Unix Epoch ms)
 * | Version Bytes - 64 bits  | one version byte ('Human Readable') + fixed 7 random bytes
 * { Records       - Variable } A set of records up to the 'chunk size' of the
 * { Zero Padding Bytes       | Pads the file with zero val bytes up to the fixed byte size of the file
 *
 *
 */

public class HeaderOps {

    private final static Logger logger = LoggerFactory.getLogger(HeaderOps.class);

    // This is the version byte - bump this by one for each new log file format change
    private static final byte VERSION_NUM = (byte)0x01;
    // It get put on the front of this 64 bit buffer for alignment and as a magic number
    private static final byte[] VERSION_BYTES = {
            VERSION_NUM,(byte)0x5A,(byte)0xF0,(byte)0x87,(byte)0xC7,(byte)0x72,(byte)0xDF,(byte)0xD1,
    };
    private static final int VERSION_BYTES_OFFSET = Long.BYTES + Long.BYTES;

    public static final int HEADER_SIZE = VERSION_BYTES_OFFSET + VERSION_BYTES.length;

    /**
     * Prepare a header buffer
     * @param recordNumber : Record number of the first record to written to this log file
     * @param recordTimestamp : Timestamp of the first record to written to this log file
     *
     * @return A buffer containing the header for a new file
     */
    public static Buffer makeHeader( long recordNumber, long recordTimestamp)  {
        return Buffer.buffer().appendLong(recordNumber).appendLong(recordTimestamp).appendBytes(VERSION_BYTES);
    }


    /**
     * Read a Header from a buffer and perform various checks as the header is decoded
     * @param in : Vertx Buffer with a header as described above.
     * @return : The Details encoded in the header
     */
    public HeaderDetails readHeader(Buffer in) {

        final long recordNumber = in.getLong(0);
        final long timestamp = in.getLong(Long.BYTES);
        final byte[] fileVersionBytes = in.getBytes(VERSION_BYTES_OFFSET, VERSION_BYTES_OFFSET +VERSION_BYTES.length);

        if ( fileVersionBytes[0] != VERSION_NUM ) {
            String msg = "Wrong version number in file " + fileVersionBytes[0] +
                    " current version is " + VERSION_NUM;
            throw new MewException(msg);
        }

        // header is bad but version is good so log and try to continue
        if ( !Arrays.equals(fileVersionBytes,VERSION_BYTES) ) {
            logger.error("Corrupt log file header - corrupt or incomplete write or read");
        }

        return new HeaderDetails(recordNumber,timestamp);
    }


    public class HeaderDetails {
        private final byte version = VERSION_NUM;
        private final long recordNumber;
        private final long timestamp;

        public HeaderDetails(long recordNumber, long timestamp) {
            this.recordNumber = recordNumber;
            this.timestamp = timestamp;
        }

        public byte getVersion() { return version; }
        public long getRecordNumber() { return recordNumber; }
        public long getTimestamp() { return timestamp; }
    }

}

package io.mewbase.server.impl.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.MewException;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.LogReadStream;
import io.mewbase.server.impl.BasicFile;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Public methods always accessed from same event loop
 * <p>
 * Package protected methods accessed from event loop of publisher
 * <p>
 * Created by tim on 22/10/16.
 */
public class LogReadStreamImpl implements LogReadStream {

    private final static Logger logger = LoggerFactory.getLogger(LogReadStreamImpl.class);

    private final LogImpl fileLog;
    private final SubDescriptor subDescriptor;
    private final Context context;
    private final int readBufferSize;
    private final Queue<BufferedRecord> buffered = new LinkedList<>();
    private final FramingOps framing = new FramingOps();

    private BiConsumer<Long, BsonObject> handler;
    private Consumer<Throwable> exceptionHandler;

    private boolean paused;
    private boolean closed;
    private long deliveredSeq = -1;
    private boolean retro;
    private long fileStreamRecordNum;
    private boolean ignoreFirst;
    private int fileNumber;

    private int fileReadPos;
    private BasicFile streamFile;
    private int fileSize;
    private RecordParser parser;

    private final static int HEADER_SENTINAL = -1;
    private int recordSize = HEADER_SENTINAL;
    private Buffer headerBuffer = null;

    public LogReadStreamImpl(LogImpl fileLog, SubDescriptor subDescriptor, int readBufferSize,
                             int fileSize) {
        this.fileLog = fileLog;
        this.subDescriptor = subDescriptor;
        this.context = Vertx.currentContext();
        this.readBufferSize = readBufferSize;
        this.fileSize = fileSize;
        resetParser();
    }

    @Override
    public void exceptionHandler(Consumer<Throwable> handler) {
        this.exceptionHandler = handler;
    }

    @Override
    public void handler(BiConsumer<Long, BsonObject> handler) {
        this.handler = handler;
    }

    @Override
    public synchronized void start() {
        checkContext();
        if (subDescriptor.getStartEventNum() != SubDescriptor.DEFAULT_START_NUM) {
            goRetro(false, subDescriptor.getStartEventNum());
        } if (subDescriptor.getStartTimestamp() != SubDescriptor.DEFAULT_START_TIME) {
            final long runStreamFromStart = 0;
            goRetro(false, runStreamFromStart);
        } else {
            fileLog.readdSubHolder(this);
        }
    }

    @Override
    public synchronized void pause() {
        paused = true;
    }

    @Override
    public synchronized void resume() {
        if (!paused) {
            return;
        }
        paused = false;
        if (!buffered.isEmpty()) {
            while (true) {
                BufferedRecord br = buffered.poll();
                if (br == null) {
                    break;
                }
                handle0(br.pos, br.bson);
                if (paused) {
                    return;
                }
            }
        }
        if (!retro && fileLog.getLastWrittenSeq() > deliveredSeq) {
            // Missed message(s)
            goRetro(true, deliveredSeq);
        }
    }

    private void goRetro(boolean ignoreFirst, long pos) {
        fileLog.removeSubHolder(this);
        retro = true;
        openFileStream(pos, ignoreFirst);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        paused = true;
        closed = true;
        fileLog.removeSubHolder(this);
        if (streamFile != null) {
            streamFile.close();
        }
    }

    public synchronized boolean isRetro() {
        return retro;
    }

    boolean matches(BsonObject bsonObject) {
        return true;
    }

    synchronized void handle(long pos, BsonObject bsonObject) {
        if (paused) {
            return;
        }
        if (pos <= deliveredSeq) {
            // This can happen if the stream is retro and a message is persisted, delivered from file, then
            // the stream re-added then the message delivered live, so we can just ignore it
            return;
        }
        handle0(pos, bsonObject);
    }

    private void resetParser() {
        parser = RecordParser.newFixed(FramingOps.HEADER_SIZE, this::handleRec);
    }

    private void handle0(long recordNumber, BsonObject bsonObject) {
        if (handler != null) {
            handler.accept(recordNumber, bsonObject);
            deliveredSeq = recordNumber;
        } else {
            throw new IllegalStateException("No handler");
        }
    }

    private void openFileStream(long recordNumber, boolean ignoreFirst) {
        this.ignoreFirst = ignoreFirst;
        this.fileStreamRecordNum = recordNumber;
        // Open a file at the record number
        FileOps.FileCoord coord = fileLog.getCoordOfRecord(recordNumber);
        fileLog.openFile(coord.fileNumber).handle((bf, t) -> {
            if (t == null) {
                streamFile = bf;
                fileNumber = coord.fileNumber;
                fileReadPos = coord.filePos;
                scheduleRead();
            } else {
                handleException(t);
            }
            return null;
        });
    }


    /**
     * This method interacts with the parser to grab for each record
     * 1) Header - checksum plus the size elements from the body
     * 2) Remainder of body and magic number
     * Until the first int of the header is 0
     * @param buff
     */
    private void handleRec(Buffer buff) {

        if (recordSize == HEADER_SENTINAL) {
            // Got a header so
            // check for padding (alt checksum)
            int possPadding = buff.getInt(0);
            if (possPadding == 0) {
                // Padding at end of file
                // so just keep looking for ints until we get to end of file at which point the
                // parser is reset to look for 'header size' again
                parser.fixedSizeMode(Integer.BYTES);
            } else {
                // Looks like a valid header so store the header ready to put the whole buffer back
                // together again when the body comes back in.
                headerBuffer = buff;
                recordSize = buff.getIntLE(FramingOps.CHECKSUM_SIZE);
                // the size includes the size part of the header so we need ignore that and include the magic at the end
                parser.fixedSizeMode(recordSize - Integer.BYTES  + FramingOps.MAGIC_SIZE);
            }
        } else {
            // Got a body and magic number
            // join the header and the rest of the body back up
            // unframe and send the body to be processed
            if (recordSize != 0) {
                final ByteBuf headerAndSize = headerBuffer.getByteBuf();
                final ByteBuf bodyRemaining = buff.getByteBuf();
                // “Thus strangely are our souls constructed, and by slight ligaments are we bound to prosperity and ruin.”
                final Buffer full = Buffer.buffer(Unpooled.wrappedBuffer(headerAndSize, bodyRemaining));
                try {
                    final Buffer body = framing.unframe(full);
                    handleBody(body);
                } catch (MewException badRead) {
                    logger.error("Bad read from stream", badRead, badRead.getErrorCode());
                }
                // set up for the next header
                parser.fixedSizeMode(FramingOps.HEADER_SIZE);
                recordSize = HEADER_SENTINAL;
                headerBuffer = null; // gc hint
            }
        }
    }


    private synchronized void handleBody(Buffer buffer) {
        if (closed) {
            return;
        }

        BsonObject bson = new BsonObject(buffer);

        if (ignoreFirst) {
            ignoreFirst = false;
        } else {
            long lastWrittenRecordNumber = fileLog.getLastWrittenSeq();
            if (fileStreamRecordNum <= lastWrittenRecordNumber) {
                if (paused) {
                    buffered.add(new BufferedRecord(fileStreamRecordNum, bson));
                } else {
                    handle0(fileStreamRecordNum, bson);
                    // The handler could have closed it
                    if (closed) {
                        return;
                    }
                }
            }
            if (fileStreamRecordNum == lastWrittenRecordNumber) {
                // Need to lock to prevent messages sneaking in before we read the stream
                synchronized (fileLog) {
                    fileStreamRecordNum = fileLog.getLastWrittenSeq();
                    if (fileStreamRecordNum == lastWrittenRecordNumber) {
                        // We've got to the head
                        retro = false;
                        streamFile.close();
                        streamFile = null;
                        resetParser();
                        fileLog.readdSubHolder(this);
                    }
                }
            }
        }
        ++fileStreamRecordNum;
    }

    private void handleException(Throwable t) {
        if (exceptionHandler != null) {
            exceptionHandler.accept(t);
        } else {
            logger.error("Failed to read", t);
        }
    }

    private void doRead() {
        try {
            if (streamFile != null) {
                // Could have been set to null if previous read gets the head
                Buffer readBuff = Buffer.buffer(readBufferSize);
                CompletableFuture<Void> cf = streamFile.read(readBuff, readBufferSize, fileReadPos);
                cf.handle((v, t) -> {
                    if (t == null) {
                        boolean endOfFile = readBuff.length() < readBufferSize;
                        if (readBuff.length() > 0) {
                            parser.handle(readBuff);
                        }
                        fileReadPos += readBuff.length();
                        if (streamFile != null && endOfFile && fileReadPos == fileSize) {
                            // We read a whole file
                            moveToNextFile();
                        } else if (!paused) {
                            scheduleRead();
                        }
                    } else {
                        handleException(t);
                    }
                    return null;
                });
            }
        } catch (RejectedExecutionException e) {
            // Can happen if pool is being shutdown
            logger.warn("Read rejected as pool being shutdown", e);
        }
    }

    private void moveToNextFile() {
        if (closed) {
            return;
        }
        streamFile.close();
        streamFile = null;
        resetParser();
        int headFileNumber = fileLog.getFileNumber();
        if (headFileNumber < fileNumber) {
            logger.warn("Invalid file number {} head {}", fileNumber, headFileNumber);
            return;
        }
        openFileStream((fileNumber + 1) * fileSize, false);
    }

    private void scheduleRead() {
        fileLog.scheduleOp(this::doRead);
    }

    private static final class BufferedRecord {
        final long pos;
        final BsonObject bson;

        BufferedRecord(long pos, BsonObject bson) {
            this.pos = pos;
            this.bson = bson;
        }
    }

    // Sanity check - this should always be executed using the correct context
    private void checkContext() {
        if (Vertx.currentContext() != context) {
            throw new IllegalStateException("Wrong context! " + Vertx.currentContext() + " expected: " + context);
        }
    }

}


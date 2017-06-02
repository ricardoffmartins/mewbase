package io.mewbase.server.impl.log;

import io.mewbase.bson.BsonObject;
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
    private BiConsumer<Long, BsonObject> handler;
    private Consumer<Throwable> exceptionHandler;

    private boolean paused;
    private boolean closed;
    private long deliveredPos = -1;
    private boolean retro;
    private long fileStreamPos;
    private boolean ignoreFirst;
    private int fileNumber;
    private int fileReadPos;
    private BasicFile streamFile;
    private int fileSize;
    private RecordParser parser;

    private final static int HEADER_SENTINAL = -1;
    private final static int HEADER_AND_SIZE = HeaderOps.HEADER_OFFSET + Integer.BYTES;
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
        if (subDescriptor.getStartPos() != SubDescriptor.DEFAULT_START_POS) {
            goRetro(false, subDescriptor.getStartPos());
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
        if (!retro && fileLog.getLastWrittenPos() > deliveredPos) {
            // Missed message(s)
            goRetro(true, deliveredPos);
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
        if (pos <= deliveredPos) {
            // This can happen if the stream is retro and a message is persisted, delivered from file, then
            // the stream re-added then the message delivered live, so we can just ignore it
            return;
        }
        handle0(pos, bsonObject);
    }

    private void resetParser() {
        parser = RecordParser.newFixed(HEADER_AND_SIZE, this::handleRec);
    }

    private void handle0(long pos, BsonObject bsonObject) {
        if (handler != null) {
            handler.accept(pos, bsonObject);
            deliveredPos = pos;
        } else {
            throw new IllegalStateException("No handler");
        }
    }

    private void openFileStream(long pos, boolean ignoreFirst) {
        this.ignoreFirst = ignoreFirst;
        this.fileStreamPos = pos;
        // Open a file
        LogImpl.FileCoord coord = fileLog.getCoord(pos);
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
     * This method interacts with the parser to grab
     * 1) Header
     * 2) Body
     * Until the first int of the header is 0
     * @param buff
     */
    private void handleRec(Buffer buff) {

        if (recordSize == HEADER_SENTINAL) {
            // Got a header so
            // check for padding (alt magic number)
            int possPadding = buff.getInt(0);
            if (possPadding == 0) {
                // Padding at end of file
                // so just keep looking for ints until we get to end of file at which point the
                // parser is reset to look for 'header plus size' again
                parser.fixedSizeMode(Integer.BYTES);
            } else {
                // Assuming magic number found
                // Looks like a valid header so store the header ready to put the whole buffer back
                // together again when the body comes back in.
                headerBuffer = buff;
                recordSize = buff.getIntLE(HeaderOps.HEADER_OFFSET) - Integer.BYTES; // we already read the size from the body
                parser.fixedSizeMode(recordSize);
            }
        } else {
            // Got a body so
            // join the header and the rest of the body back up
            // do all the header checks and send the body to be processed
            if (recordSize != 0) {
                final ByteBuf headerAndSize = headerBuffer.getByteBuf();
                final ByteBuf bodyRemaining = buff.getByteBuf();
                // “Thus strangely are our souls constructed, and by slight ligaments are we bound to prosperity and ruin.”
                final ByteBuf full = Unpooled.wrappedBuffer(headerAndSize, bodyRemaining);
                final Buffer frame = HeaderOps.readHeader(Buffer.buffer(full));
                handleFrame(frame);

                // set up for the next header
                parser.fixedSizeMode(HEADER_AND_SIZE);
                recordSize = HEADER_SENTINAL;
                headerBuffer = null; // gc hint
            }
        }
    }

    private synchronized void handleFrame(Buffer buffer) {
        if (closed) {
            return;
        }
        // TODO bit clunky - need to add size back in so it can be decoded, improve this!
        // DONE - at the cost of some complexity in handleRec
//        int bl = buffer.length() + 4;
//        Buffer buff2 = Buffer.buffer(bl);
//        buff2.appendIntLE(bl).appendBuffer(buffer);
        BsonObject bson = new BsonObject(buffer);
        if (ignoreFirst) {
            ignoreFirst = false;
        } else {
            long lwep = fileLog.getLastWrittenEndPos();
            if (fileStreamPos <= lwep) {
                if (paused) {
                    buffered.add(new BufferedRecord(fileStreamPos, bson));
                } else {
                    handle0(fileStreamPos, bson);
                    // The handler could have closed it
                    if (closed) {
                        return;
                    }
                }
            }
            if (fileStreamPos == lwep) {
                // Need to lock to prevent messages sneaking in before we read the stream
                synchronized (fileLog) {
                    lwep = fileLog.getLastWrittenEndPos();
                    if (fileStreamPos == lwep) {
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
        fileStreamPos += HeaderOps.HEADER_OFFSET + buffer.length();
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


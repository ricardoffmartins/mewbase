package io.mewbase.server.impl.log;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.MewException;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.ServerOptions;
import io.mewbase.server.impl.BasicFile;
import io.mewbase.server.impl.FileAccess;
import io.mewbase.server.impl.Protocol;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static io.mewbase.server.impl.log.FileOps.*;


/**
 * Created by tim on 07/10/16.
 */
public class LogImpl implements Log {

    private final static Logger logger = LoggerFactory.getLogger(LogImpl.class);

    private final Vertx vertx;
    private final FileAccess faf;
    private final String channel;
    private final ServerOptions options;
    private final Set<LogReadStreamImpl> fileLogStreams = new ConcurrentHashSet<>();
    private final FramingOps framing = new FramingOps();

    private BasicFile currWriteFile;
    private BasicFile nextWriteFile;
    private int fileNumber = 0; // Number of log file containing current head
    private int filePos = 0;    // Position of head in head file

    private AtomicLong nextWrittenSeq = new AtomicLong();  // The Seq number of the record to be written
    private AtomicLong lastWrittenSeq = new AtomicLong();  // Seq Number of last record written.
    private long expectedSeq;

    private CompletableFuture<Void> nextFileCF;
    private final PriorityQueue<WriteHolder> pq = new PriorityQueue<>();
    private CompletableFuture<Void> startRes;
    private long flushTimerID = -1;
    private boolean closed;


    public LogImpl(Vertx vertx, FileAccess faf, ServerOptions options, String channel) {
        this.vertx = vertx;
        this.channel = channel;
        this.options = options;
        this.faf = faf;
        if (options.getMaxLogChunkSize() < 1) {
            throw new IllegalArgumentException("maxLogChunkSize must be > 1");
        }
        if (options.getPreallocateSize() < 0) {
            throw new IllegalArgumentException("PreallocateSize must be >= 0");
        }
        if (options.getMaxRecordSize() < 1) {
            throw new IllegalArgumentException("maxRecordSize must be > 1");
        }
        if (options.getMaxRecordSize() > options.getMaxLogChunkSize()) {
            throw new IllegalArgumentException("maxRecordSize must be <= maxLogChunkSize");
        }
        if (options.getPreallocateSize() > options.getMaxLogChunkSize()) {
            throw new IllegalArgumentException("preallocateSize must be <= maxLogChunkSize");
        }
    }

    public synchronized CompletableFuture<Void> start() {
        closed = false;
        logger.trace("Starting file log " + this);
        if (startRes != null) {
            return startRes;
        }

        checkAndLoadFiles();
        File currFile = getFile(options.getLogsDir(),channel,fileNumber);
        CompletableFuture<Void> cfCreate = null;
        if (!currFile.exists()) {
            if (fileNumber == 0 && filePos == 0) {
                // This is OK, new log
                logger.trace("Creating new log file for channel {}", channel);
                // Create a new first file
                cfCreate = createAndFillFile(getFileName(channel,0));
            }
        }
        final File cFile = currFile;
        // Now open the BasicFile
        CompletableFuture<BasicFile> cf;
        if (cfCreate != null) {
            cf = cfCreate.thenCompose(v -> faf.openBasicFile(cFile));
        } else {
            cf = faf.openBasicFile(currFile);
        }
        startRes = cf.thenAccept(bf -> {
            currWriteFile = bf;
            logger.trace("Started file log for channel {}", channel);
        }).thenRun(this::scheduleFlush);

        return startRes;
    }


    private synchronized void flush() {
        if (!closed) {
            if (currWriteFile == null) {
                // Sanity check
                throw new IllegalStateException("No currWriteFile");
            }
            logger.trace("Flushing log file");
            currWriteFile.flush().thenRun(this::scheduleFlush);
        }
    }

    private synchronized void scheduleFlush() {
        flushTimerID = vertx.setTimer(options.getLogFlushInterval(), tid -> flush());
    }

    @Override
    public synchronized LogReadStream subscribe(SubDescriptor subDescriptor) {
        if (subDescriptor.getStartEventNum() < -1) {
            throw new IllegalArgumentException("start event number must be >= -1");
        }
        if (subDescriptor.getStartEventNum() > getLastWrittenSeq()) {
            throw new IllegalArgumentException("start event number cannot be greater than highest current record");
        }
        return new LogReadStreamImpl(this, subDescriptor,
                options.getReadBufferSize(), options.getMaxLogChunkSize());
    }

    @Override
    public synchronized CompletableFuture<Long> append(BsonObject obj) {
        // Encode the BsonObject and add a frame
        Buffer record = framing.frame( obj.encode() );

        // Total length of the record to write to the file
        int len = record.length();

        if (len > options.getMaxRecordSize()) {
            throw new MewException("Record too long " + len + " max " + options.getMaxRecordSize());
        }

        long timestamp = obj.getLong(Protocol.RECEV_TIMESTAMP);

        final CompletableFuture<Long> cf;

        int remainingSpace = options.getMaxLogChunkSize() - filePos;

        final int spaceRequired = record.length();
        if (spaceRequired > remainingSpace) {
            if (remainingSpace > 0) {
                // Write into the remaining space so all log chunk files are same size
                Buffer buffer = Buffer.buffer(new byte[remainingSpace]);
                append0(buffer.length(), buffer);
            }
            // Move to next file or start first file
            if (nextWriteFile != null) {
                logger.trace("Moving to next log file");
                currWriteFile = nextWriteFile;
                filePos = 0;
                fileNumber++;
                nextWriteFile = null;
            } else {
                logger.warn("Eager create of next file too slow, nextFileCF {}", nextFileCF);
                checkCreateNextFile();
                // Next file creation is in progress, just wait for it
                cf = new CompletableFuture<>();
                nextFileCF.thenAccept(v -> {
                    // When complete just call append again
                    CompletableFuture<Long> again = append(obj);
                    again.handle((pos, t) -> {
                        if (t != null) {
                            cf.completeExceptionally(t);
                        } else {
                            cf.complete(pos);
                        }
                        return null;
                    });
                });
                return cf;
            }
        }

        // Everything is set up to write so write this record away under the current number and increment
        // to move to the next record number to write
        long seq = nextWrittenSeq.getAndIncrement();
        if (filePos == 0) {
            // First record for this file so add the header
            Buffer header = HeaderOps.makeHeader(seq,timestamp);
            cf = append0(header.length(), header).thenApply(f  -> {
                append0(len, record);
                return seq;
            } );
       } else {
            cf = append0(len, record).thenApply(f -> { return seq; });
       }
        // now send the event to all of the current subscribers ensuring that
        // the events dont 'jump the queue' if for example the log is currently playing
        // back historic event (retro mode)
        cf.thenApply(pos -> {
            sendToSubsOrdered(seq, obj);
            return seq;
        });
        checkCreateNextFile();
        return cf;
    }

    protected synchronized void sendToSubsOrdered(long seq, BsonObject obj) {
        // Writes can complete in a different order to which they were submitted, we need to reorder to ensure
        // records are delivered in the correct order
        if (seq == expectedSeq) {
            sendToSubs(seq, obj);
        } else {
            // Out of order
            pq.add(new WriteHolder(seq, obj));
        }
        while (true) {
            WriteHolder head = pq.peek();
            if (head != null && head.seq == expectedSeq) {
                pq.poll();
                sendToSubs(head.seq, head.obj);
            } else {
                break;
            }
        }
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        closed = true;
        if (flushTimerID != -1) {
            vertx.cancelTimer(flushTimerID);
            flushTimerID = -1;
        }
        CompletableFuture<Void> ret;
        if (currWriteFile != null) {
            ret = currWriteFile.close();
            ret = ret.thenCompose( v ->  {logger.trace("Closed current log file for channel " + channel);
                return CompletableFuture.completedFuture(null);
            } );
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        if (nextWriteFile != null) {
            ret = ret.thenCompose(v -> nextWriteFile.close());
        }
        if (nextFileCF != null) {
            CompletableFuture<Void> ncf = nextFileCF;
            ret = ret.thenCompose(v -> ncf);
        }

        return ret;
    }

    @Override
    public synchronized int getFileNumber() {
        return fileNumber;
    }

    void removeSubHolder(LogReadStreamImpl stream) {
        fileLogStreams.remove(stream);
    }

    void readdSubHolder(LogReadStreamImpl stream) {
        fileLogStreams.add(stream);
    }

    public long getLastWrittenSeq() { return lastWrittenSeq.get(); }

    /**
     * Given a record number get the file coordinates for this record from the logs
     */
    FileCoord getCoordOfRecord(long recordNumber) {
       return FileOps.getCoordOfRecord(options.getLogsDir(),channel,recordNumber);
    }


    CompletableFuture<BasicFile> openFile(int fileNumber) {
        File file = new File(options.getLogsDir(), getFileName(channel,fileNumber));
        return faf.openBasicFile(file);
    }

    void scheduleOp(Runnable runner) {
        faf.scheduleOp(runner);
    }

    private void sendToSubs(long seq, BsonObject bsonObject) {
        synchronized (this) {
            ++expectedSeq;
            lastWrittenSeq.set(seq);
        }
        for (LogReadStreamImpl stream : fileLogStreams) {
            if (stream.matches(bsonObject)) {
                try {
                    stream.handle(seq, bsonObject);
                } catch (Throwable t) {
                    logger.error("Failed to send to subs", t);
                }
            }
        }
    }

    private CompletableFuture<Long> append0(int len, Buffer record) {
        int  writePos = filePos;
        filePos += len;
        return currWriteFile.append(record, writePos).thenApply(v -> (long)filePos);
    }


    private synchronized void checkCreateNextFile() {
        // We create a next file when the current file is half written
        if (nextFileCF == null && nextWriteFile == null && filePos > options.getMaxLogChunkSize() / 2) {
            nextFileCF = new CompletableFuture<>();
            CompletableFuture<Void> cfNext = createAndFillFile(getFileName(channel,fileNumber + 1));
            CompletableFuture<BasicFile> cfBf = cfNext.thenCompose(v -> {
                File next = getFile(options.getLogsDir(),channel,fileNumber + 1);
                return faf.openBasicFile(next);
            });
            cfBf.handle((bf, t) -> {
                synchronized (LogImpl.this) {
                    if (t == null) {
                        nextWriteFile = bf;
                        if (nextFileCF == null) {
                            logger.error("nextFileCF is null");
                        }
                        nextFileCF.complete(null);
                        nextFileCF = null;
                    } else {
                        logger.error("Failed to create next file", t);
                    }
                    return null;
                }
            }).exceptionally(t -> {
                logger.error(t.getMessage(), t);
                return null;
            });
        }
    }

    private CompletableFuture<Void> createAndFillFile(String fileName) {
        AsyncResCF<Void> cf = new AsyncResCF<>();
        File next = new File(options.getLogsDir(), fileName);
        vertx.executeBlocking(fut -> {
            FileOps.createAndFillFileBlocking(next, options.getPreallocateSize());
            fut.complete(null);
        }, false, cf);
        return cf;
    }


    /**
     * Check that the logs on the file system are in good shape and find the
     * file coordinates to start writing from.
     */
    private void checkAndLoadFiles() {
        fileNumber = checkAndGetLastLogFile(options,channel);
        // file store is in good order so set up the state variables by reading the current file.
        FileCoord coords = getCoordOfLastRecord(options.getLogsDir(),channel,fileNumber);

        lastWrittenSeq.set(coords.recordNumber);
        nextWrittenSeq.set(coords.recordNumber);
        expectedSeq = coords.recordNumber;
        filePos = coords.filePos;
    }



    private static final class WriteHolder implements Comparable<WriteHolder> {
        final long seq;
        final BsonObject obj;

        public WriteHolder(long seq, BsonObject obj) {
            this.seq = seq;
            this.obj = obj;
        }

        @Override
        public int compareTo(WriteHolder other) {
            return Long.compare(this.seq, other.seq);
        }
    }


}

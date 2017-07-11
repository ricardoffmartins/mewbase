package io.mewbase.server.impl.doc.lmdb;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.DocReadStream;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;

import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;

/**
 * Created by tim on 29/12/16.
 */
public class LmdbReadStream implements DocReadStream {

    private final static Logger logger = LoggerFactory.getLogger(LmdbReadStream.class);

    // TODO make configurable
    private static final int MAX_DELIVER_BATCH = 100;

    private final LmdbBinderFactory binderFactory;

    private final Txn txn;
    private final CursorIterator<ByteBuffer> cursorItr;  // in lmdbjava cursor is the main abstraction
    private final Iterator<CursorIterator.KeyVal<ByteBuffer>> itr;
    // attempt to give thread affinity to the txn
    private final WorkerExecutor exec;
    private final String POOL_NAME = "READ_STREAM_THREAD";
    private final int SINGLE_THREAD = 1;

    private final Predicate<BsonObject> filter;

    private Consumer<BsonObject> handler;

    private boolean paused;
    private boolean handledOne;
    private boolean closed;

    private static AtomicLong openTxnCount = new AtomicLong();


    LmdbReadStream(LmdbBinderFactory binderFactory, Dbi<ByteBuffer> db, Predicate<BsonObject> filter) {
        this.binderFactory = binderFactory;
        // System.out.println("Open txn :" + openTxnCount.incrementAndGet());
        this.txn = binderFactory.getEnv().txnRead(); // set uo a read transaction
        this.cursorItr = db.iterate(txn, FORWARD);
        this.itr = cursorItr.iterable().iterator();
        this.filter = filter;
        this.exec = binderFactory.getVertx().createSharedWorkerExecutor(POOL_NAME, SINGLE_THREAD);
    }

    @Override
    public void exceptionHandler(Consumer<Throwable> handler) {
    }

    @Override
    public void handler(Consumer<BsonObject> handler) {
        this.handler = handler;
    }

    @Override
    public synchronized void start() {
        printThread();
        runIterNextAsync();
    }

    @Override
    public synchronized void pause() {
        printThread();
        paused = true;
    }

    @Override
    public synchronized void resume() {
        printThread();
        paused = false;
        runIterNextAsync();
    }

    @Override
    public synchronized void close() {
        printThread();
        if (!closed) {
            // Following comment may no longer apply to lmdbjava
            // Beware calling tx.close() if the database/env object is closed can cause a core dump:
            cursorItr.close();
           //  System.out.println("Closed txn :" + openTxnCount.decrementAndGet());
            txn.close();
            closed = true;
        }
    }

    public synchronized boolean hasMore() {
        return itr.hasNext();
    }

    private void printThread() {
        //logger.trace("Thread is {}", Thread.currentThread());
    }

    // TODO - shouldn't this be on a worker thread?
    // as of 10/7/17 made a local single threaded Vert.x execution context which stops
    // all seg faults
    private void runIterNextAsync() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            iterNext();
            fut.complete(null);
        }, res);
    }


    private synchronized void iterNext() {
        printThread();
        if (paused || closed) {
            return;
        }
        for (int i = 0; i < MAX_DELIVER_BATCH; i++) {
            if (itr.hasNext()) {
                final CursorIterator.KeyVal<ByteBuffer> kv = itr.next();
                // Copy bytes from LMDB managed memory to vert.x buffer
                Buffer buffer = Buffer.buffer(kv.val().remaining());
                buffer.setBytes(0, kv.val());
                BsonObject doc = new BsonObject(buffer);
                if (handler != null && filter.test(doc)) {
                    handler.accept(doc);
                    handledOne = true;
                    if (paused) {
                        return;
                    }
                }
            } else {
                if (!handledOne) {
                    // Send back an empty result
                    handler.accept(null);
                }
                close();
                return;
            }
        }
        runIterNextAsync();
    }

}

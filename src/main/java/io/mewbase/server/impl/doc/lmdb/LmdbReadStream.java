package io.mewbase.server.impl.doc.lmdb;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.DocReadStream;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;

import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
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
    private final WorkerExecutor exec;

    private final Predicate<BsonObject> filter;

    private Consumer<BsonObject> handler;

    private boolean hasMore;
    private boolean paused;
    private boolean handledOne;
    private boolean closed;


    LmdbReadStream(LmdbBinderFactory binderFactory, Dbi<ByteBuffer> db, Predicate<BsonObject> filter, WorkerExecutor exec) {
        this.binderFactory = binderFactory;
        this.txn = binderFactory.getEnv().txnRead(); // set up a read transaction
        this.cursorItr = db.iterate(txn, FORWARD);
        this.itr = cursorItr.iterable().iterator();
        this.filter = filter;
        this.exec = exec;
        hasMore = itr.hasNext();    // check if the CursorIterator has any content before resetting txn
        txn.reset();    // we only need to have the transaction active while we read items under the Cursor (Iterator).
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
        runIterNextAsync();
    }

    @Override
    public synchronized void pause() {
        paused = true;
    }

    @Override
    public synchronized void resume() {
        paused = false;
        runIterNextAsync();
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            cursorItr.close();
            txn.close();
            closed = true;
        }
    }

    public synchronized boolean hasMore()  { return hasMore; }

    // Exec is managed by BinderFactory to maintain thread affinity
    private void runIterNextAsync() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            iterNext();
            fut.complete(null);
        }, res);
    }


    private synchronized void iterNext() {

        if (paused || closed) {
            return;
        }
        for (int i = 0; i < MAX_DELIVER_BATCH; i++) {
            // before we access the data via the cursor it is necessary to
            txn.renew();
            if (hasMore) {
                final CursorIterator.KeyVal<ByteBuffer> kv = itr.next();
                // Copy bytes from LMDB managed memory to vert.x buffer
                Buffer buffer = Buffer.buffer(kv.val().remaining());
                buffer.setBytes(0, kv.val());
                hasMore = itr.hasNext();
                txn.reset(); // got data so release the txn
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
                close(); // closes the active txn.
                return;
            }
        }
        runIterNextAsync();
    }

}

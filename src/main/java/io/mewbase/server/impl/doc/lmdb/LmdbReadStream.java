package io.mewbase.server.impl.doc.lmdb;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.DocReadStream;
import io.vertx.core.buffer.Buffer;

import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

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
    private final Function<BsonObject, Boolean> matcher;
    private Consumer<BsonObject> handler;
    private boolean paused;
    private boolean handledOne;
    private boolean closed;


    LmdbReadStream(LmdbBinderFactory binderFactory, Dbi<ByteBuffer> db, Function<BsonObject, Boolean> matcher) {
        this.binderFactory = binderFactory;
        this.txn = binderFactory.getEnv().txnRead(); // set uo a read transaction
        this.cursorItr = db.iterate(txn, FORWARD);
        this.itr = cursorItr.iterable().iterator();
        this.matcher = matcher;
        ;
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

    // TODO shouldn't this be on a worker thread?
    private synchronized void iterNext() {
        printThread();
        if (paused || closed) {
            return;
        }
        for (int i = 0; i < MAX_DELIVER_BATCH; i++) {
            if (itr.hasNext()) {
                final CursorIterator.KeyVal<ByteBuffer> kv = itr.next();
                // lift it from LMDB side into local memory
                byte [] local = new byte[kv.val().remaining()];
                kv.val().get(local);
                BsonObject doc = new BsonObject(Buffer.buffer(local));
                if (handler != null && matcher.apply(doc)) {
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

    private void runIterNextAsync() {
        binderFactory.getVertx().runOnContext(v -> iterNext());
    }
}

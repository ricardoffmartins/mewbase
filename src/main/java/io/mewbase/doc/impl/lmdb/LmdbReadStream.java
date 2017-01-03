package io.mewbase.doc.impl.lmdb;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.DocReadStream;
import io.vertx.core.buffer.Buffer;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Transaction;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 29/12/16.
 */
public class LmdbReadStream implements DocReadStream {

    // TODO make configurable
    private static final int MAX_DELIVER_BATCH = 100;

    private final LmdbBinderFactory binderFactory;
    private final Transaction tx;
    private final EntryIterator iter;
    private final Function<BsonObject, Boolean> matcher;
    private Consumer<BsonObject> handler;
    private boolean paused;
    private boolean hasMore;
    private boolean handledOne;
    private boolean closed;

    LmdbReadStream(LmdbBinderFactory binderFactory, Database db, Function<BsonObject, Boolean> matcher) {
        this.binderFactory = binderFactory;
        this.tx = binderFactory.getEnv().createReadTransaction();
        this.iter = db.iterate(tx);
        this.matcher = matcher;
        this.hasMore = iter.hasNext();
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
        iterNext();
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
        iterNext();
    }

    @Override
    public synchronized void close() {
        printThread();
        if (!closed) {
            iter.close();
            // Beware calling tx.close() if the database/env object is closed can cause a core dump:
            // https://github.com/deephacks/lmdbjni/issues/78
            tx.close();
            closed = true;
        }
    }

    public synchronized boolean hasMore() {
        return hasMore;
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
            if (iter.hasNext()) {
                Entry entry = iter.next();

                byte[] val = entry.getValue();
                BsonObject doc = new BsonObject(Buffer.buffer(val));
                if (handler != null && matcher.apply(doc)) {
                    hasMore = iter.hasNext();
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
        binderFactory.getVertx().runOnContext(v -> iterNext());
    }
}

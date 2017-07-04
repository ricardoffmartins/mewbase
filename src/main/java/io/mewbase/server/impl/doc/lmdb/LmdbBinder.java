package io.mewbase.server.impl.doc.lmdb;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.Binder;
import io.mewbase.server.DocReadStream;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.buffer.Buffer;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.nio.ByteBuffer.allocateDirect;


/**
 * Created by tim on 29/12/16.
 */
public class LmdbBinder implements Binder {

    private final static Logger logger = LoggerFactory.getLogger(LmdbBinder.class);

    private final LmdbBinderFactory binderFactory;
    private final String name;
    private Dbi<ByteBuffer> db;
    private AsyncResCF<Void> startRes;

    public LmdbBinder(LmdbBinderFactory binderFactory, String name) {
        this.binderFactory = binderFactory;
        this.name = name;
    }

    @Override
    public DocReadStream getMatching(Function<BsonObject, Boolean> matcher) {
        return new LmdbReadStream(binderFactory, db, matcher);
    }

    @Override
    public CompletableFuture<BsonObject> get(String id) {
        AsyncResCF<BsonObject> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            // in order to do a read we have to do it under a txn so use
            // try with resource to get the auto close magic.
            try (Txn<ByteBuffer> txn = binderFactory.getEnv().txnRead()) {
                ByteBuffer key = getKey(id);
                final ByteBuffer found = db.get(txn, key);
                if (found != null) {
                    byte [] local = new byte[txn.val().remaining()];
                    txn.val().get(local);
                    BsonObject doc = new BsonObject(Buffer.buffer(local));
                    fut.complete(doc);
                } else {
                    fut.complete(null);
                }
            } // do not try, do!
        }, res);
        return res;
    }

    @Override
    public CompletableFuture<Void> put(String id, BsonObject doc) {
        AsyncResCF<Void> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            ByteBuffer key = getKey(id);
            byte[] valBytes = doc.encode().getBytes();
            final ByteBuffer val = allocateDirect(valBytes.length);
            val.put(valBytes).flip();
            db.put(key, val);
            fut.complete(null);
        }, res);
        return res;
    }

    @Override
    public CompletableFuture<Boolean> delete(String id) {
        AsyncResCF<Boolean> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            ByteBuffer key = getKey(id);
            boolean deleted = db.delete(key);
            fut.complete(deleted);
        }, res);
        return res;
    }

    @Override
    public CompletableFuture<Void> close() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            db.close();
            binderFactory.getEnv().sync(true);
            fut.complete(null);
        }, res);
        return res;
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        // TODO test this! - Deals with race where start is called before previous start is complete
        if (startRes == null) {
            startRes = new AsyncResCF<>();
            binderFactory.getExec().executeBlocking(fut -> {
                logger.trace("Opening lmdb database " + name);
                db = binderFactory.getEnv().openDbi(name, DbiFlags.MDB_CREATE );
                logger.trace("Opened lmdb database " + name);
                fut.complete(null);
            }, startRes);
        }
        return startRes;
    }

    @Override
    public String getName() {
        return name;
    }

    private ByteBuffer getKey(String id) {
        final ByteBuffer key = allocateDirect(binderFactory.getEnv().getMaxKeySize());
        key.put(id.getBytes(StandardCharsets.UTF_8)).flip();
        return key;
    }

}
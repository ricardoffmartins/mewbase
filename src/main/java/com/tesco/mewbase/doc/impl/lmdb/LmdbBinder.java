package com.tesco.mewbase.doc.impl.lmdb;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.server.Binder;
import com.tesco.mewbase.server.DocReadStream;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.buffer.Buffer;
import org.fusesource.lmdbjni.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by tim on 29/12/16.
 */
public class LmdbBinder implements Binder {

    private final static Logger logger = LoggerFactory.getLogger(LmdbBinder.class);

    private final LmdbBinderFactory binderFactory;
    private final String name;
    private Database db;
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
        logger.trace("Callling get on binder: " + this);
        AsyncResCF<BsonObject> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            byte[] key = getKey(id);
            byte[] val = db.get(key);
            if (val != null) {
                BsonObject obj = new BsonObject(Buffer.buffer(val));
                fut.complete(obj);
            } else {
                fut.complete(null);
            }
        }, res);
        return res;
    }

    @Override
    public CompletableFuture<Void> put(String id, BsonObject doc) {
        AsyncResCF<Void> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            byte[] key = getKey(id);
            byte[] val = doc.encode().getBytes();
            db.put(key, val);
            fut.complete(null);
        }, res);
        return res;
    }

    @Override
    public CompletableFuture<Boolean> delete(String id) {
        AsyncResCF<Boolean> res = new AsyncResCF<>();
        binderFactory.getExec().executeBlocking(fut -> {
            byte[] key = getKey(id);
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
        // Deals with race where start is called before previous start is complete
        // TODO test this!
        if (startRes == null) {
            startRes = new AsyncResCF<>();
            binderFactory.getExec().executeBlocking(fut -> {
                logger.trace("Opening lmdb database " + name);
                db = binderFactory.getEnv().openDatabase(name);
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

    private byte[] getKey(String id) {
        // TODO probably a better way to do this
        return id.getBytes(StandardCharsets.UTF_8);
    }

}
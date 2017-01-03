package com.tesco.mewbase.doc.impl.lmdb;

import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.server.Binder;
import com.tesco.mewbase.server.BinderFactory;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.fusesource.lmdbjni.Constants;
import org.fusesource.lmdbjni.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 29/12/16.
 */
public class LmdbBinderFactory implements BinderFactory {

    private final static Logger logger = LoggerFactory.getLogger(LmdbBinderFactory.class);

    // TODO make configurable
    private static final String LMDB_DOCMANAGER_POOL_NAME = "mewbase.docmanagerpool";
    private static final int LMDB_DOCMANAGER_POOL_SIZE = 10;
    private static final int MAX_DBS = 128;
    private static final long MAX_DB_SIZE = 1024L * 1024L * 1024L * 1024L; // 1 Terabyte

    private final String docsDir;
    private final Vertx vertx;
    private final WorkerExecutor exec;
    private Env env;

    public LmdbBinderFactory(String docsDir, Vertx vertx) {
        logger.trace("Starting lmdb binder factory with docs dir: " + docsDir);
        this.docsDir = docsDir;
        this.vertx = vertx;
        exec = vertx.createSharedWorkerExecutor(LMDB_DOCMANAGER_POOL_NAME, LMDB_DOCMANAGER_POOL_SIZE);
    }

    @Override
    public CompletableFuture<Void> start() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            File fDocsDir = new File(docsDir);
            createIfDoesntExists(fDocsDir);
            env = new Env();
            env.setMaxDbs(MAX_DBS);
            env.setMapSize(MAX_DB_SIZE);
            env.open(fDocsDir.getPath(), Constants.NOTLS);
            fut.complete(null);
        }, res);
        return res;
    }

    @Override
    public Binder createBinder(String binderName) {
        return new LmdbBinder(this, binderName);
    }

    @Override
    public CompletableFuture<Void> close() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            env.close();
            exec.close();
            fut.complete(null);
            logger.trace("closed lmdb binder factory " + LmdbBinderFactory.this);
        }, res);
        return res;
    }

    Vertx getVertx() {
        return vertx;
    }

    WorkerExecutor getExec() {
        return exec;
    }

    Env getEnv() {
        return env;
    }

    private void createIfDoesntExists(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new MewException("Failed to create dir " + dir);
            }
        }
    }


}

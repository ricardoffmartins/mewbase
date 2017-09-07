package io.mewbase.server.impl.doc.lmdb;


import io.mewbase.server.Binder;
import io.mewbase.server.ServerOptions;
import io.mewbase.server.impl.BinderFactory;
import io.mewbase.util.AsyncResCF;
import io.mewbase.util.SizedExecutorPool;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;


import org.lmdbjava.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static org.lmdbjava.EnvFlags.MDB_NOTLS;

/**
 * Created by tim on 29/12/16.
 */
public class LmdbBinderFactory implements BinderFactory {

    private final static Logger logger = LoggerFactory.getLogger(LmdbBinderFactory.class);

    private static final String LMDB_DOCMANAGER_POOL_NAME = "mewbase.docmanagerpool";

    private final int executorPoolSize = 16;
    private final SizedExecutorPool singleExecPool;

    private final String docsDir;
    private final int maxDBs;
    private final long maxDBSize;
    private final Vertx vertx;
    private final WorkerExecutor exec;
    private Env<ByteBuffer> env;

    public LmdbBinderFactory(ServerOptions serverOptions, Vertx vertx) {
        logger.trace("Starting lmdb binder factory with docs dir: " + serverOptions.getDocsDir());
        this.docsDir = serverOptions.getDocsDir();
        this.maxDBs = serverOptions.getMaxBinders();
        this.maxDBSize = serverOptions.getMaxBinderSize();
        this.vertx = vertx;
        this.singleExecPool = new SizedExecutorPool(vertx,LMDB_DOCMANAGER_POOL_NAME,executorPoolSize);

        // This exec is only used for opening or closing the env
        exec = vertx.createSharedWorkerExecutor(LMDB_DOCMANAGER_POOL_NAME, 1);
    }

    @Override
    public CompletableFuture<Void> start() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            File fDocsDir = new File(docsDir);
            createIfDoesntExists(fDocsDir);
            env = Env.<ByteBuffer>create()
                    .setMapSize(maxDBSize)
                    .setMaxDbs(maxDBs)
                    .setMaxReaders(1024)
                    .open(fDocsDir, Integer.MAX_VALUE, MDB_NOTLS);
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

    WorkerExecutor getSingleWorkerExecutor() {
        return singleExecPool.getWorkerExecutor() ;
    }

    Vertx getVertx() {
        return vertx;
    }

    Env getEnv() {
        return env;
    }

    private void createIfDoesntExists(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create dir " + dir);
            }
        }
    }

}

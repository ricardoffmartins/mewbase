package io.mewbase.binders.impl.lmdb;


import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;

import io.mewbase.server.MewbaseOptions;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.lmdbjava.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.util.stream.Collectors;
import java.util.stream.Stream;


import static org.lmdbjava.EnvFlags.MDB_NOTLS;


public class LmdbBinderStore implements BinderStore {

    private final static Logger logger = LoggerFactory.getLogger(LmdbBinderStore.class);

    private final Vertx vertx;

    public static final String LMDB_DOCMANAGER_POOL_NAME = "binderstore";

    private final ConcurrentMap<String, Binder> binders = new ConcurrentHashMap<>();

    private final String docsDir;
    private final int maxDBs;
    private final long maxDBSize;

    private final WorkerExecutor exec;
    private Env<ByteBuffer> env;


    public LmdbBinderStore() { this(new MewbaseOptions()); }

    public LmdbBinderStore(MewbaseOptions mewbaseOptions) {
        this(mewbaseOptions, Vertx.vertx());
    }

    public LmdbBinderStore(MewbaseOptions mewbaseOptions, Vertx vertx) {

        this.vertx = vertx;

        this.docsDir = mewbaseOptions.getDocsDir();
        this.maxDBs = mewbaseOptions.getMaxBinders();
        this.maxDBSize = mewbaseOptions.getMaxBinderSize();

        // this thread opens and closes the database environment
        this.exec = vertx.createSharedWorkerExecutor(LMDB_DOCMANAGER_POOL_NAME, 1);

        AsyncResCF<Void> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
                    logger.info("Starting LMDB binder store with docs dir: " + docsDir);
                    File fDocsDir = new File(docsDir);
                    createIfDoesntExists(fDocsDir);
                    this.env = Env.<ByteBuffer>create()
                            .setMapSize(maxDBSize)
                            .setMaxDbs(maxDBs)
                            .setMaxReaders(1024)
                            .open(fDocsDir, Integer.MAX_VALUE, MDB_NOTLS);

                    // get the names all the current binders and open them
                    List<byte[]> names = env.getDbiNames();
                    names.stream().map( name -> open(new String(name)));
                    fut.complete(null);
                }, res);
    }


    @Override
    public CompletableFuture<Binder> open(String name) {
        AsyncResCF<Binder> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            Binder binder = binders.computeIfAbsent(name, k -> new LmdbBinder(k,env,vertx)) ;
            fut.complete(binder);
        }, res);
        return res;
    }


    @Override
    public CompletableFuture<Binder> get(String name) {
        AsyncResCF<Binder> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            Binder binder = binders.get(name);
            if (binder == null) {
                fut.fail("Attempt to get non opened binder " + name + " from store");
            } else {
                fut.complete(binder);
            }
        }, res);
        return res;
    }


    @Override
    public Stream<Binder> binders() {
        return binders.values().stream();
    }

    @Override
    public Stream<String> binderNames() {
        return binders.keySet().stream();
    }

    @Override
    public CompletableFuture<Void> delete(String name) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> close() {
        AsyncResCF<Boolean> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            //Set<CompletableFuture<Void>> all = binders().map(binder -> ((LmdbBinder)binder).close()).collect(Collectors.toSet());
            try {
               // CompletableFuture.allOf(all.toArray(new CompletableFuture[all.size()])).get();
                Stream<CompletableFuture<Void>> all = binders().map(binder -> ((LmdbBinder)binder).close());

            } catch (Exception e) {
                logger.error("Failed to close all binders.", e);
            } finally {
                env.close();
                exec.close();
            }
            fut.complete(true);
            logger.info("Closed LMDB binder store at " + docsDir);
        }, res);
        return res;
    }


    private void createIfDoesntExists(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create dir " + dir);
            }
        }
    }

}

package io.mewbase.server.impl;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.server.*;
import io.mewbase.server.impl.cqrs.CQRSManager;
import io.mewbase.server.impl.cqrs.QueryBuilderImpl;
import io.mewbase.server.impl.file.af.AFFileAccess;

import io.mewbase.server.impl.proj.ProjectionManager;

import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * Created by tim on 22/09/16.
 */
public class ServerImpl implements Server {

    private final static Logger logger = LoggerFactory.getLogger(ServerImpl.class);

    private final MewbaseOptions mewbaseOptions;
    private final boolean ownVertx;
    private final Vertx vertx;

    private final ProjectionManager projectionManager;

    private final CQRSManager cqrsManager;

    private final RESTServiceAdaptor restServiceAdaptor;

    private final BinderStore binderStore;

    private final FileAccess faf;
;

    ServerImpl(Vertx vertx, boolean ownVertx, MewbaseOptions mewbaseOptions) {
        this.vertx = vertx;
        this.ownVertx = ownVertx;
        if (vertx.isClustered()) {
            // Usage of locks in projection manager disallows clustered vert.x
            throw new IllegalStateException("Clustered Vert.x not supported");
        }
        this.mewbaseOptions = mewbaseOptions;

        this.faf = new AFFileAccess(vertx);

        this.projectionManager = new ProjectionManager(this);
        this.cqrsManager = new CQRSManager(this);

        this.restServiceAdaptor = new RESTServiceAdaptor(this);
        this.binderStore = new LmdbBinderStore(mewbaseOptions, vertx);
    }

    ServerImpl(MewbaseOptions mewbaseOptions) {
        this(Vertx.vertx(), true, mewbaseOptions);
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        return restServiceAdaptor.start();
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        this.binderStore.close();
        CompletableFuture<Void> cf = restServiceAdaptor.stop();
        if (ownVertx) {
            cf = cf.thenCompose(v -> {
                AsyncResCF<Void> cfCloseVertx = new AsyncResCF<>();
                vertx.close(cfCloseVertx);
                return cfCloseVertx;
            });
        }
        return cf;
    }

    public Vertx getVertx() {
        return vertx;
    }

    public MewbaseOptions getMewbaseOptions() {
        return mewbaseOptions;
    }


    // Binder related API


    @Override
    public CompletableFuture<Binder> createBinder(String name) {
        return binderStore.open(name);
    }

    @Override
    public CompletableFuture<Binder> getBinder(String name) {
        return binderStore.get(name);
    }

    @Override
    public Stream<String> listBinders() {
        return binderStore.binderNames();
    }



    @Override
    public ProjectionBuilder buildProjection(String projectionName) {
        return projectionManager.buildProjection(projectionName);
    }

    @Override
    public List<String> listProjections() {
        return projectionManager.listProjectionNames();
    }

    @Override
    public Projection getProjection(String projectionName) {
        return projectionManager.getProjection(projectionName);
    }

    // CQRS related API
    @Override
    public CommandHandlerBuilder buildCommandHandler(String commandName) {
        return cqrsManager.buildCommandHandler(commandName);
    }

    @Override
    public QueryBuilder buildQuery(String queryName) {
        return new QueryBuilderImpl(cqrsManager, queryName);
    }


    // REST Adaptor API

    @Override
    public Mewbase exposeCommand(String commandName, String uri, HttpMethod httpMethod) {
        restServiceAdaptor.exposeCommand(commandName, uri, httpMethod);
        return this;
    }

    @Override
    public Mewbase exposeQuery(String queryName, String uri) {
        restServiceAdaptor.exposeQuery(queryName, uri);
        return this;
    }

    @Override
    public Mewbase exposeFindByID(String binderName, String uri) {
        try {
            restServiceAdaptor.exposeFindByID(binderName, uri);
        } catch (Exception e) {
            logger.error("No binder",e);
        }
        return this;
    }

    // Impl
    CQRSManager getCqrsManager() {
        return cqrsManager;
    }




}

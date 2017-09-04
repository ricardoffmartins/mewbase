package io.mewbase.server.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.MewException;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.*;
import io.mewbase.server.impl.cqrs.CQRSManager;
import io.mewbase.server.impl.cqrs.QueryBuilderImpl;
import io.mewbase.server.impl.doc.lmdb.LmdbBinderFactory;
import io.mewbase.server.impl.file.af.AFFileAccess;
import io.mewbase.server.impl.log.FramingOps;
import io.mewbase.server.impl.log.LogImpl;
import io.mewbase.server.impl.proj.ProjectionManager;
import io.mewbase.server.impl.transport.net.NetTransport;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by tim on 22/09/16.
 */
public class ServerImpl implements Server {

    private final static Logger logger = LoggerFactory.getLogger(ServerImpl.class);

    public static final String BINDERS_BINDER_NAME = "_mb.binders";
    public static final String DURABLE_SUBS_BINDER_NAME = "_mb.durableSubs";

    private final ServerOptions serverOptions;
    private final boolean ownVertx;
    private final Vertx vertx;
    private final ProjectionManager projectionManager;
    private final CQRSManager cqrsManager;


    private final ConcurrentMap<String, CompletableFuture<Boolean>> startingBinders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Binder> binders = new ConcurrentHashMap<>();
    private final BinderFactory systemBinderFactory;

    private final FileAccess faf;

    private final RESTServiceAdaptor restServiceAdaptor;

    // The system binders
    private Binder bindersBinder;
    private Binder durableSubsBinder;

    ServerImpl(Vertx vertx, boolean ownVertx, ServerOptions serverOptions) {
        this.vertx = vertx;
        this.ownVertx = ownVertx;
        if (vertx.isClustered()) {
            // Usage of locks in projection manager disallows clustered vert.x
            throw new IllegalStateException("Clustered Vert.x not supported");
        }
        this.serverOptions = serverOptions;
        this.faf = new AFFileAccess(vertx);
        this.systemBinderFactory = new LmdbBinderFactory(serverOptions, vertx);
        this.projectionManager = new ProjectionManager(this);
        this.cqrsManager = new CQRSManager(this);
        this.restServiceAdaptor = new RESTServiceAdaptor(this);
    }

    ServerImpl(ServerOptions serverOptions) {
        this(Vertx.vertx(), true, serverOptions);
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        return startBinders()
                .thenCompose(v -> restServiceAdaptor.start());
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        CompletableFuture<Void> cf = restServiceAdaptor.stop()
                .thenCompose(v -> stopBinders());
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

    public ServerOptions getServerOptions() {
        return serverOptions;
    }

    // Binder related API

    @Override
    public List<String> listBinders() {
        return new ArrayList<>(binders.keySet());
    }

    @Override
    public Binder getBinder(String name) {
        return binders.get(name);
    }


    @Override
    public CompletableFuture<Boolean> createBinder(String name) {
        Binder binder = binders.get(name);
        if (binder != null) {
            return CompletableFuture.completedFuture(false);
        } else {
            // A bit of jiggery pokery to ensure we don't have a non started binders in the binders map
            // but we don't end up creating some binder twice
            CompletableFuture<Boolean> cfStart = new CompletableFuture<>();
            CompletableFuture<Boolean> cfPrev = startingBinders.putIfAbsent(name, cfStart);
            if (cfPrev != null) {
                return cfPrev;
            } else {
                final Binder thebinder = systemBinderFactory.createBinder(name);
                thebinder.start().thenCompose(v -> insertBinder(name)).thenAccept(v -> {
                    // Must be synchronized to prevent race
                    synchronized (ServerImpl.this) {
                        binders.put(name, thebinder);
                        startingBinders.remove(name);
                    }
                }).handle((v, t) -> {
                    if (t == null) {
                        cfStart.complete(true);
                    } else {
                        cfStart.completeExceptionally(t);
                    }
                    return null;
                });
                return cfStart;
            }
        }
    }

    public Binder getDurableSubsBinder() {
        return durableSubsBinder;
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
        restServiceAdaptor.exposeFindByID(binderName, uri);
        return this;
    }

    // Impl
    CQRSManager getCqrsManager() {
        return cqrsManager;
    }

    public CompletableFuture<Long> publishEvent(Log log, BsonObject event) {
        BsonObject record = new BsonObject();
        record.put(Protocol.RECEV_TIMESTAMP, System.currentTimeMillis());
        record.put(Protocol.RECEV_EVENT, event);
        return log.append(record);
    }

    private CompletableFuture<Void> startBinders() {
        return systemBinderFactory.start().thenCompose(v -> startSystemBinders()).thenCompose(v -> startUserBinders());
    }

    private synchronized CompletableFuture<Void> stopBinders() {
        CompletableFuture[] cfArr = new CompletableFuture[binders.size()];
        int i = 0;
        for (Binder binder : binders.values()) {
            cfArr[i++] = binder.close();
        }
        CompletableFuture<Void> all = CompletableFuture.allOf(cfArr);
        return all.thenCompose(v -> systemBinderFactory.close());
    }

    private CompletableFuture<Void> insertBinder(String binderName) {
        // TODO bit weird having the id in the object too??
        return bindersBinder.put(binderName, new BsonObject().put(Binder.ID_FIELD, binderName));
    }

    private CompletableFuture<Void> startSystemBinders() {
        bindersBinder = loadBinder(BINDERS_BINDER_NAME);
        // channelsBinder = loadBinder(CHANNELS_BINDER_NAME);
        durableSubsBinder = loadBinder(DURABLE_SUBS_BINDER_NAME);
        return CompletableFuture.allOf(bindersBinder.start(), durableSubsBinder.start());
    }

    private Binder loadBinder(String binderName) {
        Binder binder = systemBinderFactory.createBinder(binderName);
        binders.put(binderName, binder);
        return binder;
    }

    private CompletableFuture<Void> startBinders(List<String> binderNames) {
        CompletableFuture[] arr = new CompletableFuture[binderNames.size()];
        int i = 0;
        for (String binderName : binderNames) {
            logger.trace("Starting binder: " + binderName);
            Binder binder = loadBinder(binderName);
            arr[i++] = binder.start();
        }
        return CompletableFuture.allOf(arr);
    }

    private CompletableFuture<List<BsonObject>> listBinder(Binder binder) {
        DocReadStream stream = binder.getMatching(doc -> true);
        CompletableFuture<List<BsonObject>> cf = new CompletableFuture<>();
        List<BsonObject> docs = new ArrayList<>();
        if (stream.hasMore()) {
            stream.handler(doc -> {
                docs.add(doc);
                if (!stream.hasMore()) {
                    stream.close();
                    cf.complete(docs);
                }
            });
            stream.start();
        } else {
            cf.complete(docs);
        }
        return cf;
    }

    private CompletableFuture<Void> startUserBinders() {
        CompletableFuture<List<BsonObject>> docsCf = listBinder(bindersBinder);
        return docsCf.thenCompose(list -> {
            List<String> ids = list.stream().map(doc -> doc.getString(Binder.ID_FIELD)).collect(Collectors.toList());
            return startBinders(ids);
        });
    }


    // start the connection to the home nats cluster
    // TODO Start Nats client
    // private CompletableFuture<Void> startLogs() {


}

package io.mewbase.server.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.MewException;
import io.mewbase.server.impl.doc.lmdb.LmdbBinderFactory;
import io.mewbase.server.impl.file.af.AFFileAccess;
import io.mewbase.server.*;
import io.mewbase.server.impl.log.LogImpl;
import io.mewbase.server.impl.proj.ProjectionManager;
import io.mewbase.server.impl.transport.net.NetTransport;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
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
import java.util.stream.Collectors;

/**
 * Created by tim on 22/09/16.
 */
public class ServerImpl implements Server {

    private final static Logger logger = LoggerFactory.getLogger(ServerImpl.class);

    public static final String BINDERS_BINDER_NAME = "_mb.binders";
    public static final String CHANNELS_BINDER_NAME = "_mb.channels";
    public static final String DURABLE_SUBS_BINDER_NAME = "_mb.durableSubs";

    private final ServerOptions serverOptions;
    private final boolean ownVertx;
    private final Vertx vertx;
    private final ProjectionManager projectionManager;
    private final Set<Transport> transports = new ConcurrentHashSet<>();

    private final ConcurrentMap<String, CompletableFuture<Boolean>> startingBinders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Binder> binders = new ConcurrentHashMap<>();
    private final BinderFactory systemBinderFactory;

    private final FileAccess faf;
    private final ConcurrentMap<String, CompletableFuture<Boolean>> startingLogs = new ConcurrentHashMap<>();
    private final Map<String, Log> logs = new ConcurrentHashMap<>();

    // The system binders
    private Binder bindersBinder;
    private Binder channelsBinder;
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
        this.systemBinderFactory = new LmdbBinderFactory(serverOptions.getDocsDir(), vertx);
        this.projectionManager = new ProjectionManager(this);
    }

    ServerImpl(ServerOptions serverOptions) {
        this(Vertx.vertx(), true, serverOptions);
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        return startBinders().thenCompose(v -> startLogs()).thenCompose(v -> startTransports());
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        CompletableFuture<Void> cfStopTransports = stopTransports();
        CompletableFuture<Void> cfStopDocManager = stopBinders();
        CompletableFuture<Void> cfStopLogManager = stopLogs();
        CompletableFuture<Void> cf = CompletableFuture.allOf(cfStopTransports, cfStopDocManager, cfStopLogManager);
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

    // Channel related API

    @Override
    // Must be synchronized to prevent race
    public synchronized CompletableFuture<Boolean> createChannel(String channel) {
        Log log = logs.get(channel);
        if (log != null) {
            return CompletableFuture.completedFuture(false);
        } else {
            // A bit of jiggery pokery to ensure we don't have a non started log in the logs map
            // but we don't end up creating some log twice
            CompletableFuture<Boolean> cfStart = new CompletableFuture<>();
            CompletableFuture<Boolean> cfPrev = startingLogs.putIfAbsent(channel, cfStart);
            if (cfPrev != null) {
                return cfPrev;
            } else {
                final Log thelog = new LogImpl(vertx, faf, serverOptions, channel);
                thelog.start().thenCompose(v -> insertLog(channel)).thenAccept(v -> {
                    // Must be synchronized to prevent race
                    synchronized (ServerImpl.this) {
                        logs.put(channel, thelog);
                        startingLogs.remove(channel);
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

    @Override
    public List<String> listChannels() {
        return new ArrayList<>(logs.keySet());
    }

    // TODO should we really expose this?
    public Log getLog(String channel) {
        return logs.get(channel);
    }

    // Projection related API

    @Override
    public ProjectionBuilder buildProjection(String name) {
        return projectionManager.buildProjection(name);
    }

    @Override
    public List<String> listProjections() {
        return projectionManager.listProjectionNames();
    }

    @Override
    public Projection getProjection(String projectionName) {
        return projectionManager.getProjection(projectionName);
    }

    // Impl

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
        channelsBinder = loadBinder(CHANNELS_BINDER_NAME);
        durableSubsBinder = loadBinder(DURABLE_SUBS_BINDER_NAME);
        return CompletableFuture.allOf(bindersBinder.start(), channelsBinder.start(), durableSubsBinder.start());
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

    private CompletableFuture<Void> startLogs() {

        File logsDir = new File(serverOptions.getLogsDir());
        if (!logsDir.exists()) {
            if (!logsDir.mkdirs()) {
                throw new MewException("Failed to create directory " + logsDir);
            }
        }

        CompletableFuture<List<BsonObject>> docsCf = listBinder(channelsBinder);
        return docsCf.thenCompose(list -> {
            List<String> ids = list.stream().map(doc -> doc.getString(Binder.ID_FIELD)).collect(Collectors.toList());
            return startLogs(ids);
        });
    }

    private CompletableFuture<Void> stopLogs() {
        CompletableFuture[] arr = new CompletableFuture[logs.size()];
        int i = 0;
        for (Log log : logs.values()) {
            arr[i++] = log.close();
        }
        return CompletableFuture.allOf(arr);
    }

    private CompletableFuture<Void> startLogs(List<String> logNames) {
        CompletableFuture[] arr = new CompletableFuture[logNames.size()];
        int i = 0;
        for (String logName : logNames) {
            LogImpl log = new LogImpl(vertx, faf, serverOptions, logName);
            logs.put(logName, log);
            arr[i++] = log.start();
        }
        return CompletableFuture.allOf(arr);
    }

    private CompletableFuture<Void> insertLog(String logName) {
        // TODO bit weird having the id in the object too??
        return channelsBinder.put(logName, new BsonObject().put(Binder.ID_FIELD, logName));
    }

    private CompletableFuture<Void> startTransports() {
        // For now just net transport
        Transport transport = new NetTransport(vertx, serverOptions);
        transports.add(transport);
        transport.connectHandler(this::connectHandler);
        return transport.start();
    }

    private void connectHandler(TransportConnection transportConnection) {
        new ConnectionImpl(this, transportConnection, Vertx.currentContext(), serverOptions.getAuthProvider());
    }

    private CompletableFuture<Void> stopTransports() {
        CompletableFuture[] all = new CompletableFuture[transports.size()];
        int i = 0;
        for (Transport transport : transports) {
            all[i++] = transport.stop();
        }
        transports.clear();
        return CompletableFuture.allOf(all);
    }

}

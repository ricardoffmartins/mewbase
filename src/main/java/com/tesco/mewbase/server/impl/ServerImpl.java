package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.doc.impl.lmdb.LmdbDocManager;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;
import com.tesco.mewbase.log.impl.file.FileAccess;
import com.tesco.mewbase.log.impl.file.FileLogManager;
import com.tesco.mewbase.log.impl.file.faf.AFFileAccess;
import com.tesco.mewbase.projection.ProjectionManager;
import com.tesco.mewbase.projection.impl.ProjectionManagerImpl;
import com.tesco.mewbase.server.MewAdmin;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.transport.net.NetTransport;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public class ServerImpl implements Server {

    private final static Logger logger = LoggerFactory.getLogger(ServerImpl.class);

    private final ServerOptions serverOptions;
    private final boolean ownVertx;
    private final Vertx vertx;
    private final Set<ConnectionImpl> connections = new ConcurrentHashSet<>();
    private final LogManager logManager;
    private final DocManager docManager;
    private final ProjectionManager projectionManager;
    private final Set<Transport> transports = new ConcurrentHashSet<>();
    private final MewAdmin mewAdmin;

    public static final String SYSTEM_BINDER_PREFIX = "_mb.";
    public static final String DURABLE_SUBS_BINDER_NAME = SYSTEM_BINDER_PREFIX + "durableSubs";
    private static final String[] SYSTEM_BINDERS = new String[]{DURABLE_SUBS_BINDER_NAME};

    protected ServerImpl(Vertx vertx, boolean ownVertx, ServerOptions serverOptions) {
        this.vertx = vertx;
        this.ownVertx = ownVertx;
        if (vertx.isClustered()) {
            // Usage of locks in projection manager disallows clustered vert.x
            throw new IllegalStateException("Clustered Vert.x not supported");
        }
        this.serverOptions = serverOptions;
        FileAccess faf = new AFFileAccess(vertx);
        ServerOptions options = serverOptions == null ? new ServerOptions() : serverOptions;
        this.logManager = new FileLogManager(vertx, options, faf);
        this.docManager = new LmdbDocManager(serverOptions.getDocsDir(), vertx);
        this.projectionManager = new ProjectionManagerImpl(this);
        this.mewAdmin = new MewAdminImpl(this);
    }

    protected ServerImpl(ServerOptions serverOptions) {
        this(Vertx.vertx(), true, serverOptions);
    }

    @Override
    public MewAdmin admin() {
        return mewAdmin;
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        CompletableFuture[] all = new CompletableFuture[2 + SYSTEM_BINDERS.length];
        int i = 0;
        all[i++] = docManager.start();
        // Start the system binders
        for (String binder : SYSTEM_BINDERS) {
            all[i++] = docManager.createBinder(binder);
        }
        all[i] = startTransports();
        return CompletableFuture.allOf(all);
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        CompletableFuture<Void> cfStopTransports = stopTransports();
        CompletableFuture<Void> cfStopDocManager = docManager.close();
        CompletableFuture<Void> cfStopLogManager = logManager.close();
        CompletableFuture<Void> cf = CompletableFuture.allOf(cfStopTransports, cfStopDocManager, cfStopLogManager);
        connections.clear();
        if (ownVertx) {
            cf = cf.thenCompose(v -> {
                AsyncResCF<Void> cfCloseVertx = new AsyncResCF<>();
                vertx.close(cfCloseVertx);
                return cfCloseVertx;
            });
        }
        return cf;
    }

    protected void removeConnection(ConnectionImpl connection) {
        connections.remove(connection);
    }

    protected Log getLog(String channel) {
        return logManager.getLog(channel);
    }

    public DocManager docManager() {
        return docManager;
    }

    public LogManager logManager() {
        return logManager;
    }

    public ProjectionManager projectionManager() {
        return projectionManager;
    }

    public Vertx vertx() {
        return vertx;
    }

    private CompletableFuture<Void> startTransports() {
        // For now just net transport
        Transport transport = new NetTransport(vertx, serverOptions);
        transports.add(transport);
        transport.connectHandler(this::connectHandler);
        return transport.start();
    }

    private void connectHandler(TransportConnection transportConnection) {
        connections.add(new ConnectionImpl(this, transportConnection, Vertx.currentContext(),
                serverOptions.getAuthProvider()));
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

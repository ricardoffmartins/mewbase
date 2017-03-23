package io.mewbase.server;

import io.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Server extends Mewbase {

    static Server newServer(ServerOptions serverOptions) {
        return factory.newServer(serverOptions);
    }

    static Server newServer(Vertx vertx, ServerOptions serverOptions) {
        return factory.newServer(vertx, serverOptions);
    }

    static Server newServer(Vertx vertx, ClusterManager clusterManager, ServerOptions serverOptions) {
        return factory.newServer(vertx, clusterManager, serverOptions);
    }

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

    ClusterManager getClusterManager();

}

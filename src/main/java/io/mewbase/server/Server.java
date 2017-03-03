package io.mewbase.server;

import io.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Server extends Mewbase {

    long CLIENT_IDLE_TIMEOUT = 35000;

    static Server newServer(ServerOptions serverOptions) {
        return factory.newServer(serverOptions);
    }

    static Server newServer(Vertx vertx, ServerOptions serverOptions) {
        return factory.newServer(vertx, serverOptions);
    }

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

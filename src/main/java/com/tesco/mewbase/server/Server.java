package com.tesco.mewbase.server;

import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Server {

    static Server newServer(ServerOptions serverOptions) {
        return factory.newServer(serverOptions);
    }

    static Server newServer(Vertx vertx, ServerOptions serverOptions) {
        return factory.newServer(vertx, serverOptions);
    }

    MewAdmin admin();

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

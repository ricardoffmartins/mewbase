package com.tesco.mewbase.server;

import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Server {

    String ID_FIELD = "id";

    static Server newServer(ServerOptions serverOptions) {
        return factory.newServer(serverOptions);
    }

    static Server newServer(Vertx vertx, ServerOptions serverOptions) {
        return factory.newServer(vertx, serverOptions);
    }

    List<String> listBinderNames();

    Binder getBinder(String name);

    CompletableFuture<Boolean> createBinder(String name);

    CompletableFuture<Boolean> createLog(String channel);

    List<String> listLogNames();

    Log getLog(String channel);

    MewAdmin admin();

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

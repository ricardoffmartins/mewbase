package io.mewbase.server;

import io.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.List;
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

    List<String> listBinderNames();

    Binder getBinder(String name);

    CompletableFuture<Boolean> createBinder(String name);

    List<String> listChannelNames();

    CompletableFuture<Boolean> createChannel(String channel);


    // TODO not sure if we really need a separate MewAdmin interface...
    MewAdmin admin();

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();


    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

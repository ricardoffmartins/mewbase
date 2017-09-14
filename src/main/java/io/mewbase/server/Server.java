package io.mewbase.server;

import io.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;


/**
 * Created by tim on 21/09/16.
 */
public interface Server extends Mewbase {

    static Server newServer(MewbaseOptions mewbaseOptions) {
        return factory.newServer(mewbaseOptions);
    }

    static Server newServer(Vertx vertx, MewbaseOptions mewbaseOptions) {
        return factory.newServer(vertx, mewbaseOptions);
    }

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

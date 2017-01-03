package io.mewbase.server.impl;

import io.mewbase.server.Server;
import io.mewbase.server.ServerOptions;
import io.mewbase.server.spi.ServerFactory;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public class ServerFactoryImpl implements ServerFactory {
    @Override
    public Server newServer(ServerOptions serverOptions) {
        return new ServerImpl(serverOptions);
    }

    @Override
    public Server newServer(Vertx vertx, ServerOptions serverOptions) {
        return new ServerImpl(vertx, false, serverOptions);
    }
}

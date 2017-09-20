package io.mewbase.server.impl;

import io.mewbase.server.Server;
import io.mewbase.server.MewbaseOptions;
import io.mewbase.server.spi.ServerFactory;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public class ServerFactoryImpl implements ServerFactory {
    @Override
    public Server newServer(MewbaseOptions mewbaseOptions) {
        return new ServerImpl(mewbaseOptions);
    }

    @Override
    public Server newServer(Vertx vertx, MewbaseOptions mewbaseOptions) {
        return new ServerImpl(vertx, false, mewbaseOptions);
    }
}

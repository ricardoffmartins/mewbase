package io.mewbase.server.spi;

import io.mewbase.server.Server;
import io.mewbase.server.MewbaseOptions;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public interface ServerFactory {

    Server newServer(MewbaseOptions mewbaseOptions);

    Server newServer(Vertx vertx, MewbaseOptions mewbaseOptions);
}

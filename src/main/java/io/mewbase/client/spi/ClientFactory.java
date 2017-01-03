package io.mewbase.client.spi;

import io.mewbase.client.Client;
import io.mewbase.client.ClientOptions;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public interface ClientFactory {

    Client newClient(ClientOptions clientOptions);

    Client newClient(Vertx vertx, ClientOptions clientOptions);
}

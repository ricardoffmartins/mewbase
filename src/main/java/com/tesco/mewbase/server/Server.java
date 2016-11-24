package com.tesco.mewbase.server;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.function.FunctionManager;
import com.tesco.mewbase.query.QueryContext;
import com.tesco.mewbase.query.QueryManager;
import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Created by tim on 21/09/16.
 */
public interface Server extends FunctionManager {

    static Server newServer(ServerOptions serverOptions) {
        return factory.newServer(serverOptions);
    }

    boolean installQuery(String queryName, String binderName, BiConsumer<QueryContext, BsonObject> queryConsumer);

    boolean deleteQuery(String queryName);

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

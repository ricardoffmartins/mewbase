package io.mewbase.server;

import io.vertx.core.http.HttpMethod;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 11/01/17.
 */
public interface RESTServiceAdaptor {

    RESTServiceAdaptor exposeCommand(String commandName, String uri, HttpMethod httpMethod);

    RESTServiceAdaptor exposeQuery(String queryName, String uri);

    RESTServiceAdaptor exposeFindByID(String binderName, String uri);

    RESTServiceAdaptor setHost(String host);

    RESTServiceAdaptor setPort(int port);

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();
}

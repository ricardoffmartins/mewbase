package io.mewbase.server;

import io.mewbase.binders.Binder;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;
import io.vertx.core.http.HttpMethod;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Created by tim on 15/12/16.
 */
public interface Mewbase {

    // Binder related operations

    // CompletableFuture<Binder> createBinder(String binderName);

    // CompletableFuture<Binder> getBinder(String name);

    // Stream<String> listBinders();


    // Projection related operations

    // ProjectionBuilder buildProjection(String projectionName);

    // List<String> listProjections();

    // Projection getProjection(String projectionName);


    // Command handler related operations

    CommandHandlerBuilder buildCommandHandler(String commandName);


    // Query related operations

    QueryBuilder buildQuery(String queryName);


    // REST adaptor related operations

    Mewbase exposeCommand(String commandName, String uri, HttpMethod httpMethod);

    Mewbase exposeQuery(String queryName, String uri);

    Mewbase exposeFindByID(String binderName, String uri);

}

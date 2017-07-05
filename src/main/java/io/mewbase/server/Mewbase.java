package io.mewbase.server;

import io.vertx.core.http.HttpMethod;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 15/12/16.
 */
public interface Mewbase {

    // Binder related operations

    CompletableFuture<Boolean> createBinder(String binderName);

    Binder getBinder(String name);

    List<String> listBinders();

    // Channel related operations


    CompletableFuture<Boolean> createChannel(String channelName);

    Channel getChannel(String channelName);

    List<String> listChannels();

    SubsFilterBuilder buildSubsFilter(String channelName);

    // Projection related operations

    ProjectionBuilder buildProjection(String projectionName);

    List<String> listProjections();

    Projection getProjection(String projectionName);

    // Command handler related operations

    CommandHandlerBuilder buildCommandHandler(String commandName);

    // Query related operations

    QueryBuilder buildQuery(String queryName);


    // REST adaptor related operations

    Mewbase exposeCommand(String commandName, String uri, HttpMethod httpMethod);

    Mewbase exposeQuery(String queryName, String uri);

    Mewbase exposeFindByID(String binderName, String uri);

}

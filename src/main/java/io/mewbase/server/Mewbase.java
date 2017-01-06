package io.mewbase.server;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 15/12/16.
 */
public interface Mewbase {

    Binder getBinder(String name);

    List<String> listChannels();

    CompletableFuture<Boolean> createChannel(String channelName);

    List<String> listBinders();

    CompletableFuture<Boolean> createBinder(String binderName);

    ProjectionBuilder buildProjection(String name);

    List<String> listProjections();

    Projection getProjection(String projectionName);

}

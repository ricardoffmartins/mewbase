package com.tesco.mewbase.log;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 27/09/16.
 */
public interface LogManager {

    Log getLog(String channel);

    CompletableFuture<Boolean> createLog(String channel);

    CompletableFuture<Void> close();

    Set<String> getChannelNames();
}

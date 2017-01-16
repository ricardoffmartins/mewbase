package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 16/01/17.
 */
public interface ProcessBuilder {

    ProcessBuilder startWithStage(ProcessStage stage);

    ProcessBuilder thenStage(ProcessStage stage);

    ProcessBuilder thenDo(Function<BsonObject, CompletableFuture<Void>> actionFunction);

    ProcessBuilder withTimeout(long timeoutMs);

    ProcessBuilder onFailure(BiConsumer<Throwable, BsonObject> failureHandler);

    void create();
}

package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by tim on 16/01/17.
 */
public interface ProcessBuilder {

    ProcessBuilder withTimeout(long timeoutMs);

    ProcessBuilder onFailure(BiConsumer<BsonObject, Throwable> exceptionHandler);

    ProcessBuilder onTimeout(Consumer<BsonObject> timeoutHandler);

    ProcessBuilder addStage(ProcessStageDefinition processStage);

    ProcessDefinition build();
}

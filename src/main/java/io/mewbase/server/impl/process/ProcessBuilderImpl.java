package io.mewbase.server.impl.process;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.*;
import io.mewbase.server.ProcessBuilder;
import io.mewbase.server.impl.ServerImpl;

import java.lang.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by tim on 24/01/17.
 */
public class ProcessBuilderImpl implements ProcessBuilder {

    private final ProcessDefinition processDefinition = new ProcessDefinition();

    public ProcessBuilderImpl(String name) {
        processDefinition.setName(name);
    }

    @Override
    public ProcessBuilder withTimeout(long timeoutMs) {
        processDefinition.setTimeout(timeoutMs);
        return this;
    }

    @Override
    public ProcessBuilder onFailure(BiConsumer<BsonObject, Throwable> exceptionHandler) {
        processDefinition.setExceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public ProcessBuilder onTimeout(Consumer<BsonObject> timeoutHandler) {
        processDefinition.setTimeoutHandler(timeoutHandler);
        return this;
    }

    @Override
    public ProcessBuilder addStage(ProcessStageDefinition processStageDefinition) {
        processDefinition.addStage(processStageDefinition);
        return this;
    }

    @Override
    public ProcessDefinition build() {
        return processDefinition;
    }


}

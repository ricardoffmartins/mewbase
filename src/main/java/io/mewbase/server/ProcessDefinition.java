package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by tim on 24/01/17.
 */
public class ProcessDefinition {

    private String name;
    private long timeout;
    private BiConsumer<BsonObject, Throwable> exceptionHandler;
    private Consumer<BsonObject> timeoutHandler;
    private List<ProcessStageDefinition> stages = new ArrayList<>();

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public BiConsumer<BsonObject, Throwable> getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(BiConsumer<BsonObject, Throwable> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    public Consumer<BsonObject> getTimeoutHandler() {
        return timeoutHandler;
    }

    public void setTimeoutHandler(Consumer<BsonObject> timeoutHandler) {
        this.timeoutHandler = timeoutHandler;
    }

    public List<ProcessStageDefinition> getStageDefinitions() {
        return stages;
    }

    public void addStage(ProcessStageDefinition stage) {
        stages.add(stage);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

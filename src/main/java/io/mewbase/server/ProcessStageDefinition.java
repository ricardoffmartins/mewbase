package io.mewbase.server;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientOptions;
import io.mewbase.server.StageContext;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 24/01/17.
 */
public class ProcessStageDefinition {

    private String name;
    private String fromChannel;
    private ClientOptions clientOptions;
    private Function<BsonObject, Boolean> filterFunction;
    private Function<BsonObject, String> identifyFunction;
    private BiConsumer<BsonObject, StageContext> eventHandler;
    private Consumer<BsonObject> timeoutHandler;
    private Consumer<StageContext> action;
    private long timeout;

    public String getFromChannel() {
        return fromChannel;
    }

    public void setFromChannel(String fromChannel) {
        this.fromChannel = fromChannel;
    }

    public Function<BsonObject, Boolean> getFilterFunction() {
        return filterFunction;
    }

    public void setFilterFunction(Function<BsonObject, Boolean> filterFunction) {
        this.filterFunction = filterFunction;
    }

    public Function<BsonObject, String> getIdentifyFunction() {
        return identifyFunction;
    }

    public void setIdentifyFunction(Function<BsonObject, String> identifyFunction) {
        this.identifyFunction = identifyFunction;
    }

    public BiConsumer<BsonObject, StageContext> getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(BiConsumer<BsonObject, StageContext> eventHandler) {
        this.eventHandler = eventHandler;
    }

    public Consumer<StageContext> getAction() {
        return action;
    }

    public void setAction(Consumer<StageContext> action) {
        this.action = action;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public Consumer<BsonObject> getTimeoutHandler() {
        return timeoutHandler;
    }

    public void setTimeoutHandler(Consumer<BsonObject> timeoutHandler) {
        this.timeoutHandler = timeoutHandler;
    }

    public ClientOptions getClientOptions() {
        return clientOptions;
    }

    public void setClientOptions(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

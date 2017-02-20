package io.mewbase.server.impl.process;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientOptions;
import io.mewbase.server.*;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 24/01/17.
 */
public class ProcessStageBuilderImpl implements ProcessStageBuilder {

    private final ProcessStageDefinition stageDefinition = new ProcessStageDefinition();

    public ProcessStageBuilderImpl(String name) {
        stageDefinition.setName(name);
    }

    @Override
    public ProcessStageBuilder fromChannel(String channelName) {
        stageDefinition.setFromChannel(channelName);
        return this;
    }

    @Override
    public ProcessStageBuilder fromServer(ClientOptions clientOptions) {
        stageDefinition.setClientOptions(clientOptions);
        return this;
    }

    @Override
    public ProcessStageBuilder filteredWith(Function<BsonObject, Boolean> filterFunction) {
        stageDefinition.setFilterFunction(filterFunction);
        return this;
    }

    @Override
    public ProcessStageBuilder identifiedBy(Function<BsonObject, String> identifyFunction) {
        stageDefinition.setIdentifyFunction(identifyFunction);
        return this;
    }

    @Override
    public ProcessStageBuilder eventHandler(BiConsumer<BsonObject, StageContext> eventHandler) {
        stageDefinition.setEventHandler(eventHandler);
        return this;
    }

    @Override
    public ProcessStageBuilder thenDo(Consumer<StageContext> action) {
        stageDefinition.setAction(action);
        return this;
    }

    @Override
    public ProcessStageBuilder withTimeout(long timeoutMs) {
        stageDefinition.setTimeout(timeoutMs);
        return this;
    }

    @Override
    public ProcessStageDefinition build() {
        // TODO validation
        return stageDefinition;
    }

}

package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 16/01/17.
 */
public interface ProcessStageBuilder {

    /*
    Channel to consume from
     */
    ProcessStageBuilder fromChannel(String channelName);

    /*
    Filter out any events not interested in
     */
    ProcessStageBuilder filteredWith(Function<BsonObject, Boolean> filterFunction);

    /*
    Function that identifies which process stage should get the event
    function(event) -> boolean
    Only run once  - not run for every stage instance. Instances can be stored in a map
     */
    ProcessStageBuilder identifiedBy(Function<BsonObject, String> identifyFunction);

    /*
    Function that is passed the event. This is called once for each process stage instance

    When the stage is complete or fails then context.complete/fail is called
     */
    ProcessStageBuilder stageHandler(BiConsumer<BsonObject, StageContext> eventHandler);

    /*
    Create a composite stage that completes when all the stages complete
     */
    ProcessStage allOf(ProcessStage... stages);

    /*
    Create a composite stage that completes when any of the stages complete
     */
    ProcessStage anyOf(ProcessStage... stages);

    ProcessStage withTimeout(long timeoutMs);

    ProcessStage create();

}

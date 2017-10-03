package io.mewbase.projection;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 28/11/16.
 */
public interface ProjectionBuilder {

    ProjectionBuilder named(String projectionName);

    ProjectionBuilder projecting(String channelName);

    ProjectionBuilder filteredBy(Function<Event, Boolean> eventFilter);

    ProjectionBuilder onto(String binderName);

    ProjectionBuilder identifiedBy(Function<Event, String> docIDSelector);

    ProjectionBuilder as(BiFunction<BsonObject, Event, BsonObject> projectionFunction);

    Projection create();
}

package io.mewbase.projection;

import io.mewbase.bson.BsonObject;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 28/11/16.
 */
public interface ProjectionBuilder {

    ProjectionBuilder projecting(String channelName);

    ProjectionBuilder filteredBy(Function<BsonObject, Boolean> eventFilter);

    ProjectionBuilder onto(String binderName);

    ProjectionBuilder identifiedBy(Function<BsonObject, String> docIDSelector);

    ProjectionBuilder as(BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction);

    Projection create();
}

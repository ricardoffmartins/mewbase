package io.mewbase.projection.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 28/11/16.
 */
public class ProjectionBuilderImpl implements ProjectionBuilder {

    private final String projectionName;
    private final ProjectionManager projectionManager;
    private String channelName;
    private Function<BsonObject, Boolean> eventFilter = doc -> true;
    private String binderName;
    private Function<BsonObject, String> docIDSelector;
    private BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction;

    public ProjectionBuilderImpl(String projectionName, ProjectionManager projectionManager) {
        this.projectionName = projectionName;
        this.projectionManager = projectionManager;
    }

    @Override
    public ProjectionBuilder projecting(String channelName) {
        this.channelName = channelName;
        return this;
    }

    @Override
    public ProjectionBuilder filteredBy(Function<BsonObject, Boolean> eventFilter) {
        this.eventFilter = eventFilter;
        return this;
    }

    @Override
    public ProjectionBuilder onto(String binderName) {
        this.binderName = binderName;
        return this;
    }

    @Override
    public ProjectionBuilder identifiedBy(Function<BsonObject, String> docIDSelector) {
        this.docIDSelector = docIDSelector;
        return this;
    }

    @Override
    public ProjectionBuilder as(BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction) {
        this.projectionFunction = projectionFunction;
        return this;
    }

    @Override
    public Projection create() {
        if (channelName == null) {
            throw new IllegalStateException("Please specify a channel name");
        }
        if (eventFilter == null) {
            throw new IllegalStateException("Please specify an event filter");
        }
        if (binderName == null) {
            throw new IllegalStateException("Please specify a binder name");
        }
        if (docIDSelector == null) {
            throw new IllegalStateException("Please specify a document ID filter");
        }
        if (projectionFunction == null) {
            throw new IllegalStateException("Please specify a projection function");
        }

        return projectionManager.registerProjection(projectionName, channelName, eventFilter, binderName,
                docIDSelector, projectionFunction);
    }
}

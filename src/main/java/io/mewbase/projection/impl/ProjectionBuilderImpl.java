package io.mewbase.projection.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;
import io.mewbase.projection.ProjectionFactory;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 28/11/16.
 */
public class ProjectionBuilderImpl implements ProjectionBuilder {

    private final ProjectionFactoryImpl factory;

    private String projectionName;
    private String channelName;
    private String binderName;

    private Function<Event, Boolean> eventFilter = doc -> true;
    private Function<Event, String> docIDSelector;
    private BiFunction<BsonObject, Event, BsonObject> projectionFunction;


    ProjectionBuilderImpl(ProjectionFactoryImpl factory) {
        this.factory = factory;
    }

    @Override
    public ProjectionBuilder named(String projectionName) {
        this.projectionName = projectionName;
        return this;
    }

    @Override
    public ProjectionBuilder projecting(String channelName) {
        this.channelName = channelName;
        return this;
    }

    @Override
    public ProjectionBuilder filteredBy(Function<Event, Boolean> eventFilter) {
        this.eventFilter = eventFilter;
        return this;
    }

    @Override
    public ProjectionBuilder onto(String binderName) {
        this.binderName = binderName;
        return this;
    }

    @Override
    public ProjectionBuilder identifiedBy(Function<Event, String> docIDSelector) {
        this.docIDSelector = docIDSelector;
        return this;
    }

    @Override
    public ProjectionBuilder as(BiFunction<BsonObject, Event, BsonObject> projectionFunction) {
        this.projectionFunction = projectionFunction;
        return this;
    }

    @Override
    public Projection create() {
        if (projectionName == null) {
            throw new IllegalStateException("Please specify a projection name");
        }
        if (channelName == null) {
            throw new IllegalStateException("Please specify a channel name");
        }
        if (binderName == null) {
            throw new IllegalStateException("Please specify a binder name");
        }
        if (eventFilter == null) {
            throw new IllegalStateException("Please specify an event filter");
        }
        if (docIDSelector == null) {
            throw new IllegalStateException("Please specify a document ID filter");
        }
        if (projectionFunction == null) {
            throw new IllegalStateException("Please specify a projection function");
        }

        // got all of the parts in place. Now check for names
        if ( factory.isRegistered(projectionName) ) {
            throw new IllegalStateException("Non unique projection name " + projectionName);
        }

        // Use the factory to internal create the Projection.
        return factory.createProjection(projectionName,
                                        channelName,
                                        binderName,
                                        eventFilter,
                                        docIDSelector,
                                        projectionFunction);
    }
}

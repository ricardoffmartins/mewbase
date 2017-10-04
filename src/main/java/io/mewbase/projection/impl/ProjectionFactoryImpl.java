package io.mewbase.projection.impl;


import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;
import io.mewbase.projection.ProjectionFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;



public class ProjectionFactoryImpl implements ProjectionFactory {


    private final EventSource source;
    private final BinderStore store;


    private final Map<String, ProjectionImpl> projections = new ConcurrentHashMap<>();

    public ProjectionFactoryImpl(EventSource source, BinderStore store) throws Exception {
        this.source = source;
        this.store = store;
    }

    @Override
    public ProjectionBuilder builder() {
       return new ProjectionBuilderImpl(this);
    }

    /**
     * Instance a projection and register it with the factory
     * @param projectionName
     * @param channelName
     * @param binderName
     * @param eventFilter
     * @param docIDSelector
     * @param projectionFunction
     * @return
     */
    Projection createProjection(final String projectionName,
                                final String channelName,
                                final String binderName,
                                final Function<Event, Boolean> eventFilter,
                                final Function<Event, String> docIDSelector,
                                final BiFunction<BsonObject, Event, BsonObject> projectionFunction) {

        // if this projection has been applied to a document

        Subscription subs = source.subscribe(channelName, event -> {
            if (eventFilter.apply(event)) {
                String docID = docIDSelector.apply(event);
                store.open(binderName).thenAccept( binder -> {
                   binder.get(docID).thenAccept( inputDoc -> {
                       BsonObject outputDoc = projectionFunction.apply(inputDoc, event);
                       binder.put(docID, outputDoc);
                       // memorise the event that was processed in a binder
                   });
                });
            }
        });

        // register it with the
        ProjectionImpl proj = new ProjectionImpl(projectionName,subs);
        projections.put(projectionName,proj);
        return proj;
    }


    public boolean isRegistered(String projectionName) {
        return projectionNames().contains(projectionName);
    }

    public Set<String> projectionNames() {
        return projections.keySet();
    }


}

package io.mewbase.projection.impl;


import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinder;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;
import io.mewbase.projection.ProjectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;


public class ProjectionFactoryImpl implements ProjectionFactory {

    private final static Logger log = LoggerFactory.getLogger(ProjectionFactory.class);

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


        Subscription subs = source.subscribe(channelName, event -> {
            if (eventFilter.apply(event)) {
                // at the races is the event passes the filter.
                String docID = docIDSelector.apply(event);
                store.open(binderName).whenComplete( (binder, exp) -> {
                    if (exp != null ) {
                        log.error("Failed to open Binder " + binderName + " running projection " + projectionName, exp);
                    }
                    if ( binder != null) {
                        binder.get(docID).whenComplete((inputDoc, innerExp) -> {
                            // Case 1 - Something broke in the store/binder
                            if (innerExp != null) {
                                log.error("Error in binder " + binderName + " while finding " + docID, innerExp);
                            }
                            // case 2 - Nothing broke but the document doesnt exists
                            if (inputDoc == null && innerExp == null) {
                                inputDoc = new BsonObject();
                            }
                            // case 3 - We now have the doc
                            if (inputDoc != null && innerExp == null) {
                                BsonObject outputDoc = projectionFunction.apply(inputDoc, event);
                                binder.put(docID, outputDoc);
                                // TODO memorise the event that was processed in a binder
                            }
                        });
                    }
                    // this should never be the case unless something goes badly wrong in the store
                    if (exp == null && binder == null) {
                        log.error("Store failed without reporting an error");
                    }
                });
            }
        });

        // register it with the
        ProjectionImpl proj = new ProjectionImpl(projectionName,subs);
        projections.put(projectionName,proj);
        return proj;
    }


    @Override
    public boolean isProjection(String projectionName) {
        return projections.keySet().contains(projectionName);
    }

    @Override
    public Stream<String> projectionNames() {
        return projections.keySet().stream() ;
    }


}

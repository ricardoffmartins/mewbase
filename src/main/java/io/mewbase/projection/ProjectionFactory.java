package io.mewbase.projection;

import io.mewbase.binders.BinderStore;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventSource;
import io.mewbase.projection.impl.ProjectionFactoryImpl;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * ProjectionFactory can be used to get projection builders and wires them to the enclosed
 * event source and binder store
 */
public interface ProjectionFactory {

    /**
     * Given an EventSource and a BinderStore return the ability to to make ProjectionBuilders
     *
     * @param source - The EventSource
     * @param store  - A BinderStore to wire the projection to
     * @return
     * @throws Exception
     */
    static ProjectionFactory instance(EventSource source, BinderStore store) throws Exception {
        return new ProjectionFactoryImpl(source,store);
    }


    /**
     * Projections are complex so wiring is done through builders
     * @return A new builder that can be used to assemble a Projection
     */
    ProjectionBuilder builder();

}

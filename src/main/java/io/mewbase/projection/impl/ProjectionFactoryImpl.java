package io.mewbase.projection.impl;


import io.mewbase.binders.BinderStore;
import io.mewbase.eventsource.EventSource;
import io.mewbase.projection.ProjectionBuilder;
import io.mewbase.projection.ProjectionFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


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
}

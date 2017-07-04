package io.mewbase.server.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.SubsFilterBuilder;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;


public class SubsFilterBuilderImpl implements SubsFilterBuilder {

    private final String channelName;
    private final ConcurrentMap<String,ChannelFilters> filters;

    private String filterName;
    private Predicate<BsonObject> filter;


    public SubsFilterBuilderImpl(String  channelName, ConcurrentMap<String,ChannelFilters> filters) {
        this.channelName = channelName;
        this.filters = filters;
    }

    @Override
    public SubsFilterBuilder named(String filterName) {
        this.filterName = filterName;
        return this;
    }

    @Override
    public SubsFilterBuilder withFilter(Predicate<BsonObject> filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public void store() {

        if (this.filterName == null) {
            throw new IllegalStateException("Please specify a filter name (FQCN) using 'named' function");
        }
        if (this.filter == null) {
            throw new IllegalStateException("Please specify a filter predicate function using 'withFilter' function");
        }

        ChannelFilters subsFilters = filters.getOrDefault(channelName, new ChannelFilters());
        subsFilters.put(filterName,filter);
    }

}

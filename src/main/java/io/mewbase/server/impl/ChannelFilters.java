package io.mewbase.server.impl;

import io.mewbase.bson.BsonObject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * Simple Containment for a Map of FilterNames to Filter predicate functions.
 */

public class ChannelFilters {

    private final ConcurrentMap<String, Predicate<BsonObject>> filters = new ConcurrentHashMap<>();

    public void put(String filterName, Predicate<BsonObject> filter) {
        filters.put(filterName, filter);
    }

    public Predicate<BsonObject> get(String filterName) {
        return filters.get(filterName);

    }

}

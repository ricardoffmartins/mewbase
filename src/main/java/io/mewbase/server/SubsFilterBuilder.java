package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.function.Predicate;

/**
 * Created by Nige on 07/01/17.
 */
public interface SubsFilterBuilder {

    /**
     * Name a subscription filter
     * @param filterName
     * @return
     */
    SubsFilterBuilder named(String filterName);

    /**
     * Define a filter that will applied to a given channel.
     * @param subsFilter
     * @return
     */
    SubsFilterBuilder withFilter(Predicate<BsonObject> subsFilter);

    /**
     * Write the subscription filter away to the Server so that it can be
     * access later
     */
    void store();
}

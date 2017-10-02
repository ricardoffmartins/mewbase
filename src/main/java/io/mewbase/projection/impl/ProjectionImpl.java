package io.mewbase.projection.impl;


import io.mewbase.bson.BsonObject;
import io.mewbase.projection.Projection;

import java.util.function.BiFunction;
import java.util.function.Function;



class ProjectionImpl implements Projection {

    final String name;
    final String channel;
    final Function<BsonObject, Boolean> eventFilter;
    final Function<BsonObject, String> docIDSelector;
    final BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction;
   // final Binder binder;
   // final Context context;

    public ProjectionImpl(String name, String channel, String binderName, Function<BsonObject, Boolean> eventFilter,
                          Function<BsonObject, String> docIDSelector,
                          BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction) {
        this.name = name;
        this.channel = channel;
        this.eventFilter = eventFilter;
        this.docIDSelector = docIDSelector;
        this.projectionFunction = projectionFunction;
        // SubDescriptor subDescriptor = new SubDescriptor().setChannel(channel).setDurableID(name);
        // this.subscription = new ProjectionSubscription(server, subDescriptor, this::doHandle);
        // this.binder = server.getBinder(binderName).getNow(null);

    }




    @Override
    public String getName() {
        return name;
    }


}

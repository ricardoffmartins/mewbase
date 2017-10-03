package io.mewbase.projection.impl;


import io.mewbase.eventsource.Subscription;
import io.mewbase.projection.Projection;


class ProjectionImpl implements Projection {

    final String name;
    final Subscription subs;

    public ProjectionImpl(String name, Subscription subs) {
        this.name = name;
        this.subs = subs;
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public void stop() { subs.unsubscribe(); }


}

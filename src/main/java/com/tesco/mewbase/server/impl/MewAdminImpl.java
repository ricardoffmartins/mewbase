package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.projection.Projection;
import com.tesco.mewbase.projection.ProjectionBuilder;
import com.tesco.mewbase.server.MewAdmin;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 15/12/16.
 */
public class MewAdminImpl implements MewAdmin {

    private final ServerImpl server;

    public MewAdminImpl(ServerImpl server) {
        this.server = server;
    }

    @Override
    public ProjectionBuilder buildProjection(String name) {
        return server.projectionManager().buildProjection(name);
    }

    @Override
    public CompletableFuture<Boolean> createChannel(String channelName) {
        return server.logManager().createLog(channelName);
    }

    @Override
    public CompletableFuture<Boolean> createBinder(String binderName) {
        return server.docManager().createBinder(binderName);
    }

    @Override
    public Set<String> getProjectionNames() {
        return server.projectionManager().getProjectionNames();
    }

    @Override
    public Projection getProjection(String projectionName) {
        return server.projectionManager().getProjection(projectionName);
    }

    @Override
    public Set<String> getChannelNames() {
        return server.logManager().getChannelNames();
    }

    @Override
    public Set<String> getBinderNames() {
        return server.docManager().getBinderNames();
    }

}

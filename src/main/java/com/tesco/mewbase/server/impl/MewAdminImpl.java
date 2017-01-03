package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.projection.Projection;
import com.tesco.mewbase.projection.ProjectionBuilder;
import com.tesco.mewbase.server.MewAdmin;

import java.util.List;
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
        return server.getProjectionManager().buildProjection(name);
    }

    @Override
    public CompletableFuture<Boolean> createChannel(String channelName) {
        return server.createLog(channelName);
    }

    @Override
    public CompletableFuture<Boolean> createBinder(String binderName) {
        return server.createBinder(binderName);
    }

    @Override
    public List<String> listProjections() {
        return server.getProjectionManager().listProjectionNames();
    }

    @Override
    public Projection getProjection(String projectionName) {
        return server.getProjectionManager().getProjection(projectionName);
    }

    @Override
    public List<String> listChannels() {
        return server.listLogNames();
    }

    @Override
    public List<String> listBinders() {
        return server.listBinderNames();
    }

}

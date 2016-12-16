package com.tesco.mewbase.server;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.projection.Projection;
import com.tesco.mewbase.projection.ProjectionBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 15/12/16.
 */
public interface MewAdmin {

    ProjectionBuilder buildProjection(String name);

    CompletableFuture<Boolean> createChannel(String channelName);

    CompletableFuture<Boolean> createBinder(String binderName);

    List<Projection> listProjections();

    List<String> listChannels();

    List<String> listBinders();

}

package io.mewbase.projection.impl;

import io.mewbase.bson.BsonObject;

import io.mewbase.binders.Binder;
import io.mewbase.projection.Projection;
import io.mewbase.projection.ProjectionBuilder;
import io.mewbase.server.impl.ServerImpl;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Context;
import io.vertx.core.shareddata.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;


/**
 * Created by tim on 30/09/16.
 */
public class ProjectionManager {

    private final static Logger logger = LoggerFactory.getLogger(ProjectionManager.class);

    public static final String PROJECTION_STATE_FIELD = "_mb.lastSeqs";

    private final Map<String, ProjectionImpl> projections = new ConcurrentHashMap<>();

    private final ServerImpl server;

    public ProjectionManager(ServerImpl server) {
        this.server = server;
    }

// public ProjectionBuilder buildProjection(String name) {
//        return new ProjectionBuilderImpl(name, this);
//    }

//   // public List<String> listProjectionNames() {
//        return new ArrayList<>(projections.keySet());
//    }

    public Projection getProjection(String projectionName) {
        return projections.get(projectionName);
    }



    Projection registerProjection(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                                  String binderName, Function<BsonObject, String> docIDSelector,
                                  BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction) {
        if (projections.containsKey(name)) {
            throw new IllegalArgumentException("Projection " + name + " already registered");
        }
        logger.trace("Registering projection " + name);
        // TODO redo
//        ProjectionImpl holder =
//                new ProjectionImpl(name, channel, binderName, eventFilter, docIDSelector, projectionFunction);
//        projections.put(name, holder);
//        return holder;
        return null;
    }

}

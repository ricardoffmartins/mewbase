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

    public ProjectionBuilder buildProjection(String name) {
        return new ProjectionBuilderImpl(name, this);
    }

    public List<String> listProjectionNames() {
        return new ArrayList<>(projections.keySet());
    }

    public Projection getProjection(String projectionName) {
        return projections.get(projectionName);
    }

    private class ProjectionImpl implements Projection {

        final String name;
        final String channel;
        final ProjectionSubscription subscription;
        final Function<BsonObject, Boolean> eventFilter;
        final Function<BsonObject, String> docIDSelector;
        final BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction;
        final Binder binder;
        final Context context;

        public ProjectionImpl(String name, String channel, String binderName, Function<BsonObject, Boolean> eventFilter,
                              Function<BsonObject, String> docIDSelector,
                              BiFunction<BsonObject, BsonObject, BsonObject> projectionFunction) {
            this.name = name;
            this.channel = channel;
            this.eventFilter = eventFilter;
            this.docIDSelector = docIDSelector;
            this.projectionFunction = projectionFunction;
            SubDescriptor subDescriptor = new SubDescriptor().setChannel(channel).setDurableID(name);
            this.subscription = new ProjectionSubscription(server, subDescriptor, this::doHandle);
            this.binder = server.getBinder(binderName).getNow(null);
            this.context = server.getVertx().getOrCreateContext();
        }

        void doHandle(long seq, BsonObject frame) {
            context.runOnContext(v -> this.handler(seq, frame));
        }

        void handler(long seq, BsonObject event) {


            // Apply event filter
            if (!eventFilter.apply(event)) {
                return;
            }

            String docID = docIDSelector.apply(event);

            if (docID == null) {
                throw new IllegalArgumentException("No doc ID found in event " + event);
            }

            // 1. get lock
            // Before loading the doc we need to get an async lock otherwise we might load it before a previous
            // update has completed
            // TODO this can be optimised
            AsyncResCF<Lock> cfLock = new AsyncResCF<>();
            String lockName = binder.getName() + "." + docID;
            server.getVertx().sharedData().getLock(lockName, cfLock);

            cfLock
                    // 2. Get doc
                    .thenCompose(l -> binder.get(docID))

                    // 3. duplicate detection and call projection function
                    .thenCompose(doc -> {

                        // Duplicate detection
                        BsonObject lastSeqs = null;
                        if (doc == null) {
                            doc = new BsonObject().put("TODO - This bound to a random field", docID);
                        } else {
                            lastSeqs = doc.getBsonObject(PROJECTION_STATE_FIELD);
                            if (lastSeqs != null) {
                                Long processedSeq = lastSeqs.getLong(name);
                                if (processedSeq != null) {
                                    if (processedSeq >= seq) {
                                        // We've processed this one before, so ignore it
                                        logger.trace("Ignoring event " + seq + " as already processed");
                                        return CompletableFuture.completedFuture(false);
                                    }
                                }
                            }
                        }

                        if (lastSeqs == null) {
                            lastSeqs = new BsonObject();
                            doc.put(PROJECTION_STATE_FIELD, lastSeqs);
                        }

                        BsonObject updated = projectionFunction.apply(doc, event);

                        // Update the last sequence
                        lastSeqs.put(name, seq);

                        // Store the doc
                        CompletableFuture<Void> cfSaved = binder.put(docID, updated);
                        return cfSaved.thenApply(v -> true);
                    })

                    // 4. acknowledge and release lock if was processed
                    .thenAccept(processed -> {
                        if (processed) {
                           // subscription.acknowledge(seq);
                        }
                        try {
                            cfLock.get().release();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })

                    // 5. handle exceptions
                    .exceptionally(t -> {
                        logger.error("Failed in processing projection " + name, t);
                        return null;
                    });
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void pause() {
            /// subscription.pause();
        }

        @Override
        public void resume() {
            //subscription.resume();
        }

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

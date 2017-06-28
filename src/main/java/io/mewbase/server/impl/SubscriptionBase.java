package io.mewbase.server.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientOptions;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.Binder;
import io.mewbase.server.Log;
import io.mewbase.server.LogReadStream;
import io.mewbase.server.filter.FilterFactory;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.net.ClientOptionsBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;


/**
 * Created by tim on 26/09/16.
 */
public abstract class SubscriptionBase {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private static final String DURABLE_SUBS_BINDER_LAST_ACKED_FIELD = "lastAcked";

    private final ServerImpl server;
    private final SubDescriptor subDescriptor;
    private final Context ctx;
    private final Predicate<BsonObject> subsFilter;
    protected LogReadStream readStream;
    private boolean ignoreFirst;

    public SubscriptionBase(ServerImpl server,
                            SubDescriptor subDescriptor) {
        this.server = server;
        this.subDescriptor = subDescriptor;
        this.ctx = Vertx.currentContext();
        this.subsFilter = FilterFactory.makeFilter(subDescriptor.getFilterName());

        if (subDescriptor.getDurableID() != null) {
            Binder binder = server.getDurableSubsBinder();
            CompletableFuture<BsonObject> cf = binder.get(subDescriptor.getDurableID());
            cf.handle((doc, t) -> {
                if (t == null) {
                    if (doc != null) {
                        Long lastAcked = doc.getLong(DURABLE_SUBS_BINDER_LAST_ACKED_FIELD);
                        logger.trace("Restarting durable sub from (not including) {}", lastAcked);
                        if (lastAcked == null) {
                            throw new IllegalStateException("No last acked field");
                        } else {
                            subDescriptor.setStartPos(lastAcked);
                            // We don't want to redeliver the last acked event
                            ignoreFirst = true;
                        }
                    }
                    startReadStream();
                } else {
                    logger.error("Failed to lookup durable sub", t);
                }
                return null;
            }).exceptionally(t -> {
                logger.error("Failure in starting stream", t);
                return null;
            });
        } else {
            startReadStream();
        }
    }

    private void startReadStream() {
        Log log = server.getLog(subDescriptor.getChannel());
        readStream = log.subscribe(subDescriptor);
        readStream.handler(this::handleEvent0);
        readStream.start();
    }

    public void close() {
        checkContext();
        readStream.close();
    }

    // Unsubscribe deletes the durable subscription
    public void unsubscribe() {
        if (subDescriptor.getDurableID() != null) {
            server.getDurableSubsBinder().delete(subDescriptor.getDurableID());
        }
    }

    // This can be called on different threads depending on whether the frame is coming from file or direct
    private synchronized void handleEvent0(long pos, BsonObject frame) {
        if (ignoreFirst) {
            ignoreFirst = false;
            return;
        }
        // only do the Bson lookup if there is a non default timestamp and watch for the "fast fail" return
        if (subDescriptor.getStartTimestamp() != SubDescriptor.DEFAULT_START_TIME) {
            final long timeStamp = frame.getLong(Protocol.RECEV_TIMESTAMP);
            if (timeStamp < subDescriptor.getStartTimestamp()) {
                return;
            }
        }

        if (subsFilter.test(frame)) onReceiveFrame(pos, frame);
        return;
    }

    protected abstract void onReceiveFrame(long pos, BsonObject frame);

    protected void afterAcknowledge(long pos) {
        // Store durable sub last acked position
        if (subDescriptor.getDurableID() != null) {
            BsonObject ackedDoc = new BsonObject().put(DURABLE_SUBS_BINDER_LAST_ACKED_FIELD, pos);
            server.getDurableSubsBinder().put(subDescriptor.getDurableID(), ackedDoc);
        }
    }


    // Sanity check - this should always be executed using the correct context
    protected void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context! " + Vertx.currentContext() + " expected: " + ctx);
        }
    }
}

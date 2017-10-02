package io.mewbase.projection.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.impl.ServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * Created by tim on 24/11/16.
 */
public class ProjectionSubscription  {

    private final static Logger logger = LoggerFactory.getLogger(ProjectionSubscription.class);


    private final int maxUnackedEvents;
    private final BiConsumer<Long, BsonObject> frameHandler;
    private int unackedEvents;

    public ProjectionSubscription(ServerImpl server, SubDescriptor subDescriptor,
                                  BiConsumer<Long, BsonObject> frameHandler) {

        this.frameHandler = frameHandler;
        this.maxUnackedEvents = 256;
    }


}

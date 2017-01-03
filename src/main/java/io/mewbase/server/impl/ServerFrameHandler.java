package io.mewbase.server.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.common.FrameHandler;

/**
 * Created by tim on 24/09/16.
 */
public interface ServerFrameHandler extends FrameHandler {

    @Override
    default void handleResponse(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleSubResponse(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleRecev(int size, BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleQueryResult(int size, BsonObject frame) {
        throw new UnsupportedOperationException();
    }
}

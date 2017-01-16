package io.mewbase.server;

import io.mewbase.bson.BsonObject;

/**
 * Created by tim on 16/01/17.
 */
public interface StageContext {

    BsonObject getState();

    void complete();

    void fail(Exception e);
}

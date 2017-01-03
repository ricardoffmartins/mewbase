package io.mewbase.client;

import io.mewbase.bson.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public interface QueryResult {

    BsonObject document();

    void acknowledge();

    boolean isLast();
}

package com.tesco.mewbase.query;

import com.tesco.mewbase.bson.BsonObject;

/**
 * Created by tim on 24/11/16.
 */
public interface QueryContext {

    BsonObject params();

    BsonObject state();

    void emitRow(BsonObject row);
}

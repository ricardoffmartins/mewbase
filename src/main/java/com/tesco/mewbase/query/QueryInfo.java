package com.tesco.mewbase.query;

import com.tesco.mewbase.bson.BsonObject;

import java.util.function.BiConsumer;

/**
 * Created by tim on 24/11/16.
 */
public class QueryInfo {

    public final String binderName;
    public final BiConsumer<QueryContext, BsonObject> queryConsumer;

    public QueryInfo(String binderName, BiConsumer<QueryContext, BsonObject> queryConsumer) {
        this.binderName = binderName;
        this.queryConsumer = queryConsumer;
    }
}

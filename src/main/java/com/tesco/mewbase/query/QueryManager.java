package com.tesco.mewbase.query;

import com.tesco.mewbase.bson.BsonObject;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by tim on 24/11/16.
 */
public interface QueryManager {

    boolean installQuery(String queryName, String binderName, BiConsumer<QueryContext, BsonObject> queryConsumer);

    boolean deleteQuery(String queryName);

    QueryInfo getQuery(String queryName);

}

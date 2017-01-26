package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 07/01/17.
 */
public interface QueryBuilder {

    QueryBuilder from(String binderName);

    // Will be called with every doc in the binder
    // (params, context) -> boolean
    QueryBuilder documentFilter(BiFunction<BsonObject, QueryContext, Boolean> documentFilter);

    Query create();
}

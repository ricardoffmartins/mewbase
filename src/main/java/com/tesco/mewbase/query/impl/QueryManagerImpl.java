package com.tesco.mewbase.query.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.query.QueryContext;
import com.tesco.mewbase.query.QueryInfo;
import com.tesco.mewbase.query.QueryManager;
import com.tesco.mewbase.server.impl.QueryExecution;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Created by tim on 24/11/16.
 */
public class QueryManagerImpl implements QueryManager {

    private final Map<String, QueryInfo> queryInfos = new HashMap<>();

    @Override
    public synchronized boolean installQuery(String queryName, String binderName,
                                             BiConsumer<QueryContext, BsonObject> queryConsumer) {
        if (queryInfos.containsKey(queryName)) {
            return false;
        }
        queryInfos.put(queryName, new QueryInfo(binderName, queryConsumer));
        return true;
    }

    @Override
    public synchronized boolean deleteQuery(String queryName) {
        return queryInfos.remove(queryName) != null;
    }

    @Override
    public QueryInfo getQuery(String queryName) {
        return queryInfos.get(queryName);
    }


}

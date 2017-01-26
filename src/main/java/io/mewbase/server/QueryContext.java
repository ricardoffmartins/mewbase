package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 10/01/17.
 */
public class QueryContext {

    private final BsonObject params;
    private boolean complete;

    public QueryContext(BsonObject params) {
        this.params = params;
    }

    public BsonObject getParams() {
        return params;
    }

    public void complete() {
        complete = true;
    }

    public boolean isComplete() {
        return complete;
    }
}

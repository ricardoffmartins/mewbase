package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 07/01/17.
 */
public abstract class CommandContext extends CompletableFuture<Void> {

    public abstract CommandContext publishEvent(BsonObject event);

    public void complete() {
        complete(null);
    }
    
    public static void putFields(BsonObject from, BsonObject to, String... fieldNames) {
        for (String fieldName: fieldNames) {
            to.put(fieldName, from.getValue(fieldName));
        }
    }
}

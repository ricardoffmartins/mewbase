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

    public abstract String getStringField(String fieldName);

    public abstract Integer getIntegerField(String fieldName);

    public abstract Object getField(String fieldName);

    public BsonObject putFields(BsonObject bsonObject, String... fieldNames) {
        for (String fieldName: fieldNames) {
            bsonObject.put(fieldName, getField(fieldName));
        }
        return bsonObject;
    }
}

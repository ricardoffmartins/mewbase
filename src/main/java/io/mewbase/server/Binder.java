package io.mewbase.server;

import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by tim on 29/12/16.
 */
public interface Binder {

    String getName();

    /**
     * Get a document in a named binder matching the filter
     *
     * @param matcher matching projection for documents
     */
    DocReadStream getMatching(Function<BsonObject, Boolean> matcher);

    /**
     * Get a document  with the given id
     *
     * @param id the name of the document within the binder
     * @return a CompleteableFuture of the document
     */
    CompletableFuture<BsonObject> get(String id);

    /**
     * Put a document iat the given id
     *
     * @param id  the name of the document within the binder
     * @param doc the document to save
     * @return
     */
    CompletableFuture<Void> put(String id, BsonObject doc);

    /**
     * Delete a document from a binder
     *
     * @param id the name of the document within the binder
     * @return a CompleteableFuture with a Boolean set to true if successful
     */
    CompletableFuture<Boolean> delete(String id);

    CompletableFuture<Void> close();

    CompletableFuture<Void> start();

}

package io.mewbase.binders;

import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by tim on 29/12/16.
 */
public interface Binder {

    String ID_FIELD = "id";

    String getName();

    /**
     * Get a document in a named binder matching the filter
     *
     * @param filter matching projection for documents
     */
    Stream<BsonObject> getMatching(Predicate<BsonObject> filter);

    /**
     * Get a document with the given id
     *
     * @param id the name of the document within the binder
     * @return a CompleteableFuture of the document
     */
    CompletableFuture<BsonObject> get(String id);

    /**
     * Put a document at the given id
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

}

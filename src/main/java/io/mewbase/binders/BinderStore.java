package io.mewbase.binders;



import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


/**
 * Created by Nige on 14/09/17.
 */
public interface BinderStore {

    /**
     * Create a new binder of the given name.
     *
     * The return future will complete exceptionally if the store
     *  - fails to create the new binder on the backing store
     *  - already contains a binder of this name
     *
     * @param name of the Binder to create
     * @return succesfull cf if Binder is created otherwise complet
     */
    CompletableFuture<Void> create(String name);

    /**
     * Get a Binder with the given name
     *
     * @param  name of the document within the binder
     * @return a CompleteableFuture of the binder or a failed future.
     */
    CompletableFuture<Binder> get(String name);

    /**
     * Return a stream of the Binders so that maps / filters can be applied.
     *
     * @return a stream of all of the current binders
     */
    Stream<Binder> binders();

    /**
     * Return a stream of all of the names of the binders
     *
     * @return a stream of all of the current binder names
     */
    Stream<String> binderNames();

    /**
     * Delete a binder from the store
     *
     * @param  name of  binder
     * @return a CompleteableFuture with a Boolean set to true if successful
     */
    CompletableFuture<Void> delete(String name);

}

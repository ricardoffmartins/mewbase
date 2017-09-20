package io.mewbase.binders;


import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


/**
 * Created by Nige on 14/09/17.
 */
public interface BinderStore {

    /**
     * Open a new binder of the given name.
     *
     * If the binder doesnt already exist the binder wil be created.
     *
     * The return future will complete exceptionally if the store fails to create the new binder on the backing store
     *
     * @param name of the Binder to open or create and open
     * @return succesfull  if Binder is created otherwise complet
     */
    CompletableFuture<Binder> open(String name);

    /**
     * Get a Binder with the given name
     *
     * @param  name of the document within the binder
     * @return a CompleteableFuture of the binder or a failed future if the binder doesnt exist.
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
     * @return a stream of all of the current binder names.
     */
    Stream<String> binderNames();

    /**
     * Delete a binder from the store
     *
     * @param  name of  binder
     * @return a CompleteableFuture with a Boolean set to true if successful
     */
    CompletableFuture<Void> delete(String name);


    /**
     * Close the store in an orderly way ensuring that the binders are all closed and flushed to backing store.
     *
     * @return
     */
    CompletableFuture<Boolean> close();

}

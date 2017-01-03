package com.tesco.mewbase.server;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 29/12/16.
 */
public interface BinderFactory {

    Binder createBinder(String binderName);

    CompletableFuture<Void> start();

    CompletableFuture<Void> close();
}

package io.mewbase.server;

import java.util.concurrent.CompletableFuture;

/**
 * Contains all authorization info in order to perform operations
 * in the Mewbase code.
 */
public interface MewbaseUser {

    CompletableFuture<Boolean> isAuthorised(String protocolFrame);

}

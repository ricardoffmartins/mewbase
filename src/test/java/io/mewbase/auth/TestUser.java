package io.mewbase.auth;

import io.mewbase.client.MewException;
import io.mewbase.server.MewbaseUser;

import java.util.concurrent.CompletableFuture;

public class TestUser implements MewbaseUser {
    private final boolean isAuthorised;
    private final boolean throwException;

    public TestUser(boolean isAuthorised, boolean throwException) {
        this.isAuthorised = isAuthorised;
        this.throwException = throwException;
    }

    @Override
    public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();

        if (throwException) {
            cf.completeExceptionally(new MewException("Authorisation failed"));
        } else {
            cf.complete(isAuthorised);
        }


        return cf;
    }
}

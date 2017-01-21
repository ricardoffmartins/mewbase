package io.mewbase.auth;

import io.mewbase.server.MewbaseUser;

import java.util.concurrent.CompletableFuture;

public class TestUser implements MewbaseUser {
    private final boolean isAuthorised;

    public TestUser(boolean isAuthorised) {
        this.isAuthorised = isAuthorised;
    }

    @Override
    public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();

        cf.complete(isAuthorised);

        return cf;
    }
}

package io.mewbase.server.impl.auth;

import io.mewbase.server.MewbaseUser;

import java.util.concurrent.CompletableFuture;

public class DummyUser implements MewbaseUser {

    @Override
    public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();

        cf.complete(true);

        return cf;
    }
}

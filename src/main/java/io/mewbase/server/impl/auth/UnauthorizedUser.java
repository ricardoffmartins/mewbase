package io.mewbase.server.impl.auth;

import io.mewbase.server.MewbaseUser;

import java.util.concurrent.CompletableFuture;

public class UnauthorizedUser implements MewbaseUser {

    @Override
    public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {
        CompletableFuture<Boolean> unauthorisedCF = new CompletableFuture<>();

        unauthorisedCF.complete(false);

        return unauthorisedCF;
    }
}

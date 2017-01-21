package io.mewbase.server.impl.auth;

import io.mewbase.server.MewbaseUser;
import io.vertx.ext.auth.User;

import java.util.concurrent.CompletableFuture;

public class VertxUser implements MewbaseUser {

    private User user;

    public VertxUser(User user) {
        this.user = user;
    }

    @Override
    public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {

        CompletableFuture<Boolean> cf = new CompletableFuture<>();

        user.isAuthorised(protocolFrame, vertxRes -> cf.complete(vertxRes.succeeded()));

        return cf;
    }
}

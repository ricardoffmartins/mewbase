package io.mewbase.server.impl.auth;

import io.mewbase.server.MewbaseUser;
import io.mewbase.util.AsyncResCF;
import io.vertx.ext.auth.User;

import java.util.concurrent.CompletableFuture;

public class VertxUser implements MewbaseUser {

    private User user;

    public VertxUser(User user) {
        this.user = user;
    }

    @Override
    public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {
        AsyncResCF<Boolean> cf = new AsyncResCF<>();
        user.isAuthorised(protocolFrame, cf);
        return cf;
    }
}

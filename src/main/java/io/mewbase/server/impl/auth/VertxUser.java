package io.mewbase.server.impl.auth;

import io.mewbase.server.MewbaseUser;
import io.vertx.ext.auth.User;

public class VertxUser implements MewbaseUser {

    private User user;

    public VertxUser(User user) {
        this.user = user;
    }
}

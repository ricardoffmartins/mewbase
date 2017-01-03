package io.mewbase.auth.impl;

import io.mewbase.auth.MewbaseUser;
import io.vertx.ext.auth.User;

public class VertxUser implements MewbaseUser {

    private User user;

    public VertxUser(User user) {
        this.user = user;
    }
}

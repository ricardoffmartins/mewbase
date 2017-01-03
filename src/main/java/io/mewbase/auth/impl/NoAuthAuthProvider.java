package io.mewbase.auth.impl;

import io.mewbase.auth.MewbaseAuthProvider;
import io.mewbase.auth.MewbaseUser;
import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * The default auth provider implementation that does not enforce any authentication
 */
public class NoAuthAuthProvider implements MewbaseAuthProvider {

    @Override
    public CompletableFuture<MewbaseUser> authenticate(BsonObject authInfo) {
        return CompletableFuture.completedFuture(new DummyUser());
    }
}

package io.mewbase.server.impl.auth;

import io.mewbase.server.MewbaseAuthProvider;
import io.mewbase.server.MewbaseUser;
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

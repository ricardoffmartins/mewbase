package com.tesco.mewbase.auth;

import com.tesco.mewbase.ServerTestBase;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.*;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.MewAdmin;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthenticationTestBase extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(AuthenticationTestBase.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        createDirectories();
        startServer();
        setupChannelsAndBinders();
    }

    @Override
    protected void setupChannelsAndBinders() throws Exception {
        MewAdmin admin = server.admin();
        admin.createChannel(TEST_CHANNEL_1).get();
        admin.createChannel(TEST_CHANNEL_2).get();
        admin.createBinder(TEST_BINDER1).get();
    }

    @Override
    protected ServerOptions createServerOptions() {
        return super.createServerOptions()
                .setAuthProvider(createAuthProvider());
    }

    @Override
    protected ClientOptions createClientOptions() {
        return super.createClientOptions().setAuthInfo(authInfo);
    }

    protected BsonObject authInfo;

    protected MewbaseAuthProvider createAuthProvider() {
        return new TestAuthProvider();
    }

    protected Async execSimplePubSub(boolean success, TestContext context) throws Exception {
        startClient();

        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = success ? context.async() : null;

        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> async.complete();

        try {
            client.subscribe(descriptor, handler).get();
            if (!success) {
                context.fail("Should throw exception");
            }
            prod.publish(sent).get();
        } catch (ExecutionException e) {
            if (!success) {
                Throwable cause = e.getCause();
                assertTrue(cause instanceof MewException);
                MewException mcause = (MewException)cause;
                assertEquals("Authentication failed", mcause.getMessage());
                assertEquals(Client.ERR_AUTHENTICATION_FAILED, mcause.getErrorCode());
            } else {
                context.fail("Exception received");
            }
        }

        return async;
    }

}

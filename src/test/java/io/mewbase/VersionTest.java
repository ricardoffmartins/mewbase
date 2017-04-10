package io.mewbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientOptions;
import io.mewbase.server.impl.Protocol;
import io.mewbase.server.impl.ServerVersionProvider;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(VertxUnitRunner.class)
public class VersionTest extends ServerTestBase {

    private static final String MEWBASE_VERSION_TXT = "mewbase-version.txt";
    private String expectedVersion;

    @Before
    public void init() throws IOException {
        // Read the expected version from the mewbase-version.txt
        final URL resource = this.getClass().getClassLoader().getResource(MEWBASE_VERSION_TXT);
        if (resource == null) {
            throw new IllegalStateException("Cannot find the version file");
        } else {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(resource.openStream()));
            expectedVersion = in.readLine();
            in.close();
        }
    }


    @Test
    public void testVersionCompatibility() {
        Assert.assertTrue(ServerVersionProvider.isCompatibleWith(expectedVersion));
    }

    @Test
    public void testVersionMatchesExpectedVersion() {
        Assert.assertTrue(ServerVersionProvider.getVersion().equals(expectedVersion));
    }


    @Test
    public void givenConnectFrameSent_WithCorrectVersion_ThenConnectionNotClosed(TestContext testContext)
            throws Exception {
        Async async = testContext.async();

        AtomicBoolean isClosed = new AtomicBoolean();

        final String validVersion = this.expectedVersion;
        final Handler<Void> closeHandler = h -> isClosed.set(true);
        sendConnectFrame(validVersion, closeHandler);

        vertx.setTimer(500, e -> {
            testContext.assertFalse(isClosed.get(), "Expected connection not closed");
            async.complete();
        });
    }

    @Test
    public void givenConnectFrameSent_WithIncorrectVersion_ThenConnectionClosed(TestContext testContext)
            throws InterruptedException {
        final Async async = testContext.async();

        final String invalidVersion = String.valueOf(Double.MAX_VALUE);
        final Handler<Void> closeHandler = h -> async.complete();

        sendConnectFrame(invalidVersion, closeHandler);

    }

    private void sendConnectFrame(String clientVersion, Handler<Void> closeHandler) throws InterruptedException {

        ClientOptions clientOptions = new ClientOptions();
        NetClient netClient = vertx.createNetClient(clientOptions.getNetClientOptions());

        AsyncResCF<NetSocket> cf = new AsyncResCF<>();
        netClient.connect(clientOptions.getPort(), clientOptions.getHost(), cf);

        cf.thenAccept(ns -> {
            ns.closeHandler(closeHandler);

            BsonObject frame = new BsonObject();
            frame.put(Protocol.CONNECT_VERSION, clientVersion);
            frame.put(Protocol.CONNECT_AUTH_INFO, clientOptions.getAuthInfo());
            Buffer buffer = Protocol.encodeFrame(Protocol.CONNECT_FRAME, frame);
            ns.write(buffer);

        });
    }


}

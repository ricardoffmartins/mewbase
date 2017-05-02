package io.mewbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.MewbaseUser;
import io.mewbase.server.ServerOptions;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Created by vlad on 3/18/17.
 */
@RunWith(VertxUnitRunner.class)
public class ProtocolTest extends ServerTestBase {

    @Override
    protected void setupChannelsAndBinders() throws Exception {
        server.createChannel(TEST_CHANNEL_1).get();
    }

    @Override
    protected void startClient() throws Exception {
        // NOOP - don't need a client
    }

    @Override
    protected ServerOptions createServerOptions() {
        ServerOptions serverOptions = super.createServerOptions();
        serverOptions.setAuthProvider(authInfo -> {
            MewbaseUser user;
            if (authInfo.equals(expectedAuthInfo)) {
                user = new TestMewbaseUser();
            } else {
                user = null;
            }
            return CompletableFuture.completedFuture(user);
        });

        return serverOptions;
    }

    protected BsonObject expectedAuthInfo;

    @Test
    public void testConnect(TestContext testContext) throws Exception {

        String version = "0.1";
        BsonObject authInfo = new BsonObject().put("username", "tim").put("password", "aardvark");
        BsonObject frame = new BsonObject().put("version", version).put("authInfo", authInfo);
        expectedAuthInfo = authInfo;

        BsonObject expected = new BsonObject().put("type", "RESPONSE").put("frame", new BsonObject().put("ok", true));

        sendAndExpect(testContext, false, "CONNECT", frame, false, expected).get();

    }

    protected void setAuth() {
        BsonObject authInfo = new BsonObject().put("username", "tim").put("password", "aardvark");
        expectedAuthInfo = authInfo;
    }

    @Test
    public void testPublishOKNoSessionID(TestContext testContext) throws Exception {
        testPublishOK(testContext, null);
    }

    @Test
    public void testPublishOKWithSessionID(TestContext testContext) throws Exception {
        testPublishOK(testContext, 12345);
    }

    private void testPublishOK(TestContext testContext, Integer sessionID) throws Exception {
        setAuth();

        BsonObject event = new BsonObject().put("foo", "bar");
        Integer rID = 12345;

        BsonObject frame = new BsonObject().put("channel", TEST_CHANNEL_1).put("event", event)
                .put("rID", rID);

        if (sessionID != null) {
            frame.put("sessID", sessionID);
        }

        BsonObject expected =
                new BsonObject().put("type", "RESPONSE").put("frame", new BsonObject().put("ok", true).put("rID", rID));

        sendAndExpect(testContext, true, "PUB", frame, false, expected).get();
    }

    @Test
    public void testPublishNoSuchChannel(TestContext testContext) throws Exception {

        BsonObject event = new BsonObject().put("foo", "bar");
        String channelName = "ygayugasd";
        BsonObject frame = new BsonObject().put("channel", channelName).put("event", event);
        Integer rID = 12345;
        frame.put("rID", rID);
        BsonObject expected =
                new BsonObject().put("type", "RESPONSE").put("frame", new BsonObject().put("ok", false).put("rID", rID)
                        .put("errCode", 3).put("errMsg", "no such channel " + channelName));

        sendAndExpect(testContext, true, "PUB", frame, false, expected).get();
    }

    @Test
    public void testPublishInvalidSessionID(TestContext testContext) throws Exception {

        BsonObject event = new BsonObject().put("foo", "bar");
        String channelName = TEST_CHANNEL_1;
        String sessionID = "shouldn't be a string";
        BsonObject frame = new BsonObject().put("channel", channelName).put("event", event).put("sessID", sessionID);

        testPublishProtocolError(testContext, frame);
    }

    @Test
    public void testPublishMissingChannel(TestContext testContext) throws Exception {

        BsonObject event = new BsonObject().put("foo", "bar");
        BsonObject frame = new BsonObject().put("event", event);

        testPublishProtocolError(testContext, frame);
    }

    @Test
    public void testPublishMissingEvent(TestContext testContext) throws Exception {

        BsonObject frame = new BsonObject().put("channel", TEST_CHANNEL_1);

        testPublishProtocolError(testContext, frame);
    }

    private void testPublishProtocolError(TestContext testContext, BsonObject frame) throws Exception {

        setAuth();
        Integer rID = 12345;
        frame.put("rID", rID);

        sendAndExpect(testContext, true, "PUB", frame, true).get();
    }

    // Test sessID non integer etc


    class TestMewbaseUser implements MewbaseUser {

        @Override
        public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {
            return CompletableFuture.completedFuture(true);
        }
    }


    protected CompletableFuture<Void> sendAndExpect(TestContext testContext, boolean handleConnect,
                                 String frameType, BsonObject frame, boolean expectClose, BsonObject... received) {

        System.out.println("handle connect " + handleConnect);
        NetClient client = vertx.createNetClient();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        client.connect(ServerOptions.DEFAULT_PORT, "127.0.0.1", ar -> {
            if (ar.succeeded()) {
                NetSocket socket = ar.result();

                CompletableFuture<Void> cf2;
                if (handleConnect) {

                    cf2 = doConnect(testContext, socket).thenCompose(v -> {
                        System.out.println("connect done");
                        return sendAndExpect(testContext, socket, frameType, frame, expectClose, received);
                    });
                } else {
                    cf2 = sendAndExpect(testContext, socket, frameType, frame, expectClose, received);
                }
                cf2.whenComplete((v, t) -> {
                    if (t == null) {
                        cf.complete(null);
                    } else {
                        cf.completeExceptionally(t);
                    }
                });
            } else {
                testContext.fail(ar.cause());
            }
        });

        return cf;

    }

    private CompletableFuture<Void> doConnect(TestContext testContext, NetSocket socket) {
        String version = "0.1";
        BsonObject authInfo = new BsonObject().put("username", "tim").put("password", "aardvark");
        BsonObject frame = new BsonObject().put("version", version).put("authInfo", authInfo);
        expectedAuthInfo = authInfo;

        BsonObject expected = new BsonObject().put("type", "RESPONSE").put("frame", new BsonObject().put("ok", true));

        return sendAndExpect(testContext, socket, "CONNECT", frame, false, expected);
    }

    protected CompletableFuture<Void> sendAndExpect(TestContext testContext, NetSocket socket, String frameType,
                                                    BsonObject frame, boolean expectClose, BsonObject... received) {

        //Async async = testContext.async();

        System.out.println("In send and connect");

        CompletableFuture<Void> cf = new CompletableFuture<>();

        if (!expectClose) {

            AtomicInteger receivedCount = new AtomicInteger();

            RecordParser parser = RecordParser.newFixed(4, null);
            Handler<Buffer> handler = new Handler<Buffer>() {
                int size = -1;

                public void handle(Buffer buff) {
                    System.out.println("Received buff: " + buff);
                    if (size == -1) {
                        size = buff.getIntLE(0) - 4;
                        parser.fixedSizeMode(size);
                    } else {
                        Buffer buff2 = Buffer.buffer(buff.length() + 4);
                        buff2.appendIntLE(size + 4).appendBuffer(buff);
                        BsonObject bson = new BsonObject(buff2);

                        System.out.println("Received: " + bson);

                        BsonObject expected = received[receivedCount.get()];
                        testContext.assertEquals(expected, bson);
                        if (receivedCount.incrementAndGet() == received.length) {
                            //async.complete();
                            System.out.println("Completing");
                            cf.complete(null);
                        }

                        parser.fixedSizeMode(4);
                        size = -1;
                    }
                }
            };
            parser.setOutput(handler);
            socket.handler(parser);
        } else {
            socket.closeHandler(v -> {
                System.out.println("Connection has been closed");
                cf.complete(null);
            });
        }

        BsonObject bsonObject = new BsonObject().put("type", frameType);
        bsonObject.put("frame", frame);
        socket.write(bsonObject.encode());
        System.out.println("Wrote: " + bsonObject);

        return cf;

    }



}
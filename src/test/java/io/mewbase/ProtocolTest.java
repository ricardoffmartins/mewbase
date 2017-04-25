package io.mewbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientOptions;
import io.mewbase.client.MewException;
import io.mewbase.server.MewbaseAuthProvider;
import io.mewbase.server.MewbaseUser;
import io.mewbase.server.ServerOptions;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

        Async async = testContext.async();

        Thread.sleep(1000);

        String version = "0.1";
        BsonObject authInfo = new BsonObject().put("username", "tim").put("password", "aardvark");
        BsonObject frame = new BsonObject().put("version", version).put("authInfo", authInfo);
        expectedAuthInfo = authInfo;

        BsonObject expected = new BsonObject().put("type", "RESPONSE").put("frame", new BsonObject().put("ok", true));

        sendAndExpect(testContext, false, "CONNECT", frame, expected);

        //Thread.sleep(10000);

        async.await();
    }

    class TestMewbaseUser implements MewbaseUser {

        @Override
        public CompletableFuture<Boolean> isAuthorised(String protocolFrame) {
            return CompletableFuture.completedFuture(true);
        }
    }


    protected CompletableFuture<Void> sendAndExpect(TestContext testContext, boolean handleConnect,
                                 String frameType, BsonObject frame, BsonObject... received) {

        NetClient client = vertx.createNetClient();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        client.connect(ServerOptions.DEFAULT_PORT, "127.0.0.1", ar -> {
            if (ar.succeeded()) {
                NetSocket socket = ar.result();

                CompletableFuture<Void> cf2;
                if (handleConnect) {
                    cf2 = doConnect(testContext, socket);
                    cf2 = cf2.thenCompose(v -> sendAndExpect(testContext, socket, frameType, frame, received));
                } else {
                    cf2 = sendAndExpect(testContext, socket, frameType, frame, received);
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

        return sendAndExpect(testContext, socket, "CONNECT", frame, expected);
    }

    protected CompletableFuture<Void> sendAndExpect(TestContext testContext, NetSocket socket, String frameType,
                                 BsonObject frame, BsonObject... received) {

        //Async async = testContext.async();

        CompletableFuture<Void> cf = new CompletableFuture<>();

        AtomicInteger receivedCount = new AtomicInteger();

        RecordParser parser = RecordParser.newFixed(4, null);
        Handler<Buffer> handler = new Handler<Buffer>() {
            int size = -1;
            public void handle(Buffer buff) {
                //System.out.println("Received buff: " +buff);
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
                        cf.complete(null);
                    }

                    parser.fixedSizeMode(4);
                    size = -1;
                }
            }
        };
        parser.setOutput(handler);
        socket.handler(parser);

        BsonObject bsonObject = new BsonObject().put("type", frameType);
        bsonObject.put("frame", frame);
        socket.write(bsonObject.encode());
        System.out.println("Wrote: " + bsonObject);

        return cf;

    }

    class TestDescription {

        private List<Entry> list = new ArrayList<>();

        class Entry {
            final boolean isSend;
            final BsonObject bsonObject;

            public Entry(boolean isSend, BsonObject bsonObject) {
                this.isSend = isSend;
                this.bsonObject = bsonObject;
            }
        }

        void send(BsonObject bsonObject) {
            list.add(new Entry(true, bsonObject));
        }

        void receive(BsonObject bsonObject) {
            list.add(new Entry(false, bsonObject));
        }
    }


}
package io.mewbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientDelivery;
import io.mewbase.client.ClientOptions;
import io.mewbase.client.Producer;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.ServerOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.function.Consumer;

@RunWith(VertxUnitRunner.class)
public class SSLChannelsTest extends ServerTestBase {

    private final static String CERT_PATH = "src/test/resources/server-cert.pem";
    private final static String KEY_PATH = "src/test/resources/server-key.pem";

    @Override
    protected void setupChannelsAndBinders() throws Exception {
        server.createChannel(TEST_CHANNEL_1);
    }

    @Override
    protected ServerOptions createServerOptions() {
        ServerOptions serverOptions = super.createServerOptions();
        serverOptions.getNetServerOptions().setSsl(true).setPemKeyCertOptions(
                new PemKeyCertOptions()
                        .setKeyPath(KEY_PATH)
                        .setCertPath(CERT_PATH)
        );
        return serverOptions;
    }

    @Override
    protected ClientOptions createClientOptions() {
        ClientOptions clientOptions = super.createClientOptions();
        clientOptions.getNetClientOptions()
                .setSsl(true)
                .setPemTrustOptions(
                        new PemTrustOptions().addCertPath(CERT_PATH)
                );
        return clientOptions;
    }

    @Test
    public void testSimplePubSub(TestContext context) throws Exception {
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = context.async();
        long now = System.currentTimeMillis();
        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            context.assertEquals(0l, re.channelPos());
            context.assertTrue(re.timeStamp() >= now);
            BsonObject event = re.event();
            context.assertEquals(sent, event);
            async.complete();
        };

        client.subscribe(descriptor, handler).get();

        prod.publish(sent).get();
    }
}

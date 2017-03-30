package io.mewbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientOptions;
import io.mewbase.server.Server;
import io.mewbase.server.ServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by vlad on 3/18/17.
 */
@RunWith(VertxUnitRunner.class)
public class PingTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(PingTest.class);
    private final static String CERT_PATH = "src/test/resources/server-cert.pem";
    private final static String KEY_PATH = "src/test/resources/server-key.pem";
    private final static int PING_PERIOD_IN_SEC = (int) (ClientOptions.DEFAULT_PING_PERIOD / 1000);

    @Override
    protected void setupChannelsAndBinders() throws Exception {
        server.createChannel(TEST_CHANNEL_1).get();
    }

    @Test
    public void testUsualWork(TestContext context) throws Exception {
        baseTestCase(context);
    }

    @Test(expected = TimeoutException.class)
    public void testExceedTimeout(TestContext context) throws Exception {
        // Customize idle timeout
        stopServerAndClient();
        ServerOptions serverOptions = super.createServerOptions();
        serverOptions.getNetServerOptions()
                .setIdleTimeout(1);
        server = Server.newServer(vertx, serverOptions);
        server.start().get();

        startClient();
        baseTestCase(context);
    }

    public void baseTestCase(TestContext context) throws Exception {
        BsonObject sent = new BsonObject().put("foo", "bar");
        // Publish to the channel
        client.publish(TEST_CHANNEL_1, sent).get(2, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(PING_PERIOD_IN_SEC + 1);
        client.publish(TEST_CHANNEL_1, sent).get(2, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(PING_PERIOD_IN_SEC + 1);
        client.publish(TEST_CHANNEL_1, sent).get(2, TimeUnit.SECONDS);
    }
}
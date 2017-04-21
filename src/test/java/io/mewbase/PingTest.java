package io.mewbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientOptions;
import io.mewbase.client.MewException;
import io.mewbase.server.ServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Created by vlad on 3/18/17.
 */
@RunWith(VertxUnitRunner.class)
public class PingTest extends ServerTestBase {

    private int idleTimeoutSeconds;
    private long pingPeriodMs;

    @Override
    protected void setupChannelsAndBinders() throws Exception {
        server.createChannel(TEST_CHANNEL_1).get();
    }

    @Override
    protected ServerOptions createServerOptions() {
        ServerOptions options = super.createServerOptions();
        options.getNetServerOptions().setIdleTimeout(idleTimeoutSeconds);
        return options;
    }

    @Override
    protected ClientOptions createClientOptions() {
        ClientOptions clientOptions = super.createClientOptions();
        clientOptions.setPingPeriod(pingPeriodMs);
        return clientOptions;
    }

    @Test
    public void testNotClosed(TestContext testContext) throws Exception {
        stopServerAndClient();
        idleTimeoutSeconds = 2;
        pingPeriodMs = 1000;
        startServerAndClient();
        publishEvent();
        Thread.sleep(1000 * idleTimeoutSeconds * 2);
        publishEvent();
        client.publish(TEST_CHANNEL_1, new BsonObject()).get();
    }

    @Test
    public void testClosed() throws Exception {
        stopServerAndClient();
        idleTimeoutSeconds = 1;
        pingPeriodMs = 1000000;
        startServerAndClient();
        // Need one publish to lazily create connection
        publishEvent();
        Thread.sleep(1000 * idleTimeoutSeconds * 2);
        try {
            publishEvent();
            fail("Should throw exception");
        } catch (MewException e) {
            assertEquals("Connection is closed", e.getMessage());
        }
    }

    private void publishEvent() throws Exception {
        client.publish(TEST_CHANNEL_1, new BsonObject()).get();
    }
}
package io.mewbase;


import io.mewbase.server.MewbaseOptions;
import io.vertx.core.json.JsonObject;

import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetServerOptionsConverter;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class OptionsTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(OptionsTest.class);


    @Test
    public void testServerOptions() throws Exception {

        MewbaseOptions options = new MewbaseOptions();
        assertEquals(new NetServerOptions().setPort(MewbaseOptions.DEFAULT_PORT)
                .setIdleTimeout(MewbaseOptions.DEFAULT_CONNECTION_IDLE_TIMEOUT), options.getNetServerOptions());
        NetServerOptions netServerOptions2 = new NetServerOptions();
        options.setNetServerOptions(netServerOptions2);
        assertSame(netServerOptions2, options.getNetServerOptions());

        assertEquals(MewbaseOptions.DEFAULT_BINDERS_DIR, options.getDocsDir());
        String s = randomString();
        options.setDocsDir(s);
        assertEquals(s, options.getDocsDir());

        assertEquals(MewbaseOptions.DEFAULT_QUERY_MAX_UNACKED_BYTES, options.getQueryMaxUnackedBytes());
        int i = randomInt();
        options.setQueryMaxUnackedBytes(i);
        assertEquals(i, options.getQueryMaxUnackedBytes());

        assertEquals(MewbaseOptions.DEFAULT_SUBSCRIPTION_MAX_UNACKED_BYTES, options.getSubscriptionMaxUnackedBytes());
        i = randomInt();
        options.setSubscriptionMaxUnackedBytes(i);
        assertEquals(i, options.getSubscriptionMaxUnackedBytes());

        assertEquals(MewbaseOptions.DEFAULT_PROJECTION_MAX_UNACKED_EVENTS, options.getProjectionMaxUnackedEvents());
        i = randomInt();
        options.setProjectionMaxUnackedEvents(i);
        assertEquals(i, options.getProjectionMaxUnackedEvents());

        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDER_SIZE, options.getMaxBinderSize());
        long l = randomLong();
        options.setMaxBinderSize(l);
        assertEquals(l, options.getMaxBinderSize());

        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDERS, options.getMaxBinders());
        i = randomInt();
        options.setMaxBinders(i);
        assertEquals(i, options.getMaxBinders());

        assertEquals(MewbaseOptions.DEFAULT_DOC_STREAM_BATCH_SIZE, options.getDocStreamBatchSize());
        i = randomInt();
        options.setDocStreamBatchSize(i);
        assertEquals(i, options.getDocStreamBatchSize());
    }

    @Test
    public void testServerOptionsFromEmptyJson() throws Exception {
        JsonObject json = new JsonObject();
        MewbaseOptions options = new MewbaseOptions(json);
        assertEquals(MewbaseOptions.DEFAULT_BINDERS_DIR, options.getDocsDir());
        assertEquals(MewbaseOptions.DEFAULT_QUERY_MAX_UNACKED_BYTES, options.getQueryMaxUnackedBytes());
        assertEquals(MewbaseOptions.DEFAULT_SUBSCRIPTION_MAX_UNACKED_BYTES, options.getSubscriptionMaxUnackedBytes());
        assertEquals(MewbaseOptions.DEFAULT_PROJECTION_MAX_UNACKED_EVENTS, options.getProjectionMaxUnackedEvents());
        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDER_SIZE, options.getMaxBinderSize());
        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDERS, options.getMaxBinders());
        assertEquals(MewbaseOptions.DEFAULT_DOC_STREAM_BATCH_SIZE, options.getDocStreamBatchSize());
        assertEquals(new NetServerOptions(), options.getNetServerOptions());
    }

    @Test
    public void testServerOptionsFromJson() throws Exception {
        JsonObject json = new JsonObject();
        String docsDir = randomString();
        String logsDir = randomString();
        int maxLogChunkSize = randomInt();
        int maxRecordSize = randomInt();
        int preallocateSize = randomInt();
        int readBufferSize = randomInt();
        String hostName = randomString();
        int queryMaxUnackedBytes = randomInt();
        int subscriptionMaxUnackedBytes = randomInt();
        int projectionMaxUnackedEvents = randomInt();
        long logFlushInterval = randomLong();
        long maxBinderSize = randomLong();
        int maxBinders = randomInt();

        json.put("docsDir", docsDir);
        json.put("logsDir", logsDir);
        json.put("maxLogChunkSize", maxLogChunkSize);
        json.put("maxRecordSize", maxRecordSize);
        json.put("preallocateSize", preallocateSize);
        json.put("readBufferSize", readBufferSize);
        json.put("queryMaxUnackedBytes", queryMaxUnackedBytes);
        json.put("subscriptionMaxUnackedBytes", subscriptionMaxUnackedBytes);
        json.put("projectionMaxUnackedEvents", projectionMaxUnackedEvents);
        json.put("logFlushInterval", logFlushInterval);
        json.put("maxBinderSize", maxBinderSize);
        json.put("maxBinders", maxBinders);

        NetServerOptions nso = new NetServerOptions().setHost(hostName);
        JsonObject jnso = new JsonObject();
        NetServerOptionsConverter.toJson(nso, jnso);
        json.put("netServerOptions", jnso);
        MewbaseOptions options = new MewbaseOptions(json);
        assertEquals(docsDir, options.getDocsDir());

        assertEquals(queryMaxUnackedBytes, options.getQueryMaxUnackedBytes());
        assertEquals(subscriptionMaxUnackedBytes, options.getSubscriptionMaxUnackedBytes());
        assertEquals(projectionMaxUnackedEvents, options.getProjectionMaxUnackedEvents());
        assertEquals(maxBinderSize, options.getMaxBinderSize());
        assertEquals(maxBinders, options.getMaxBinders());

        assertEquals(nso, options.getNetServerOptions());
        assertEquals(hostName, options.getNetServerOptions().getHost());
    }
}

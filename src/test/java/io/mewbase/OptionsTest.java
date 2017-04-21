package io.mewbase;

import io.mewbase.client.ClientOptions;
import io.mewbase.server.ServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClientOptions;
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
    public void testClientOptions() throws Exception {

        ClientOptions options = new ClientOptions();
        assertEquals(new NetClientOptions(), options.getNetClientOptions());
        assertEquals(ClientOptions.DEFAULT_PORT, options.getPort());
        assertEquals(ClientOptions.DEFAULT_HOST, options.getHost());
        NetClientOptions netClientOptions2 = new NetClientOptions();
        options.setNetClientOptions(netClientOptions2);
        assertSame(netClientOptions2, options.getNetClientOptions());

        int i = randomInt();
        options.setPort(i);
        assertEquals(i, options.getPort());
        String s = randomString();
        options.setHost(s);
        assertEquals(s, options.getHost());
    }

    @Test
    public void testServerOptions() throws Exception {

        ServerOptions options = new ServerOptions();
        assertEquals(new NetServerOptions().setPort(ServerOptions.DEFAULT_PORT)
                .setIdleTimeout(ServerOptions.DEFAULT_CONNECTION_IDLE_TIMEOUT), options.getNetServerOptions());
        NetServerOptions netServerOptions2 = new NetServerOptions();
        options.setNetServerOptions(netServerOptions2);
        assertSame(netServerOptions2, options.getNetServerOptions());

        assertEquals(ServerOptions.DEFAULT_DOCS_DIR, options.getDocsDir());
        String s = randomString();
        options.setDocsDir(s);
        assertEquals(s, options.getDocsDir());

        assertEquals(ServerOptions.DEFAULT_MAX_LOG_CHUNK_SIZE, options.getMaxLogChunkSize());
        int i = randomInt();
        options.setMaxLogChunkSize(i);
        assertEquals(i, options.getMaxLogChunkSize());

        assertEquals(ServerOptions.DEFAULT_PREALLOCATE_SIZE, options.getPreallocateSize());
        i = randomInt();
        options.setPreallocateSize(i);
        assertEquals(i, options.getPreallocateSize());

        assertEquals(ServerOptions.DEFAULT_READ_BUFFER_SIZE, options.getReadBufferSize());
        i = randomInt();
        options.setReadBufferSize(i);
        assertEquals(i, options.getReadBufferSize());

        assertEquals(ServerOptions.DEFAULT_QUERY_MAX_UNACKED_BYTES, options.getQueryMaxUnackedBytes());
        i = randomInt();
        options.setQueryMaxUnackedBytes(i);
        assertEquals(i, options.getQueryMaxUnackedBytes());

        assertEquals(ServerOptions.DEFAULT_SUBSCRIPTION_MAX_UNACKED_BYTES, options.getSubscriptionMaxUnackedBytes());
        i = randomInt();
        options.setSubscriptionMaxUnackedBytes(i);
        assertEquals(i, options.getSubscriptionMaxUnackedBytes());

        assertEquals(ServerOptions.DEFAULT_PROJECTION_MAX_UNACKED_EVENTS, options.getProjectionMaxUnackedEvents());
        i = randomInt();
        options.setProjectionMaxUnackedEvents(i);
        assertEquals(i, options.getProjectionMaxUnackedEvents());

        assertEquals(ServerOptions.DEFAULT_LOG_FLUSH_INTERVAL, options.getLogFlushInterval());
        long l = randomLong();
        options.setLogFlushInterval(l);
        assertEquals(l, options.getLogFlushInterval());

        assertEquals(ServerOptions.DEFAULT_MAX_BINDER_SIZE, options.getMaxBinderSize());
        l = randomLong();
        options.setMaxBinderSize(l);
        assertEquals(l, options.getMaxBinderSize());

        assertEquals(ServerOptions.DEFAULT_MAX_BINDERS, options.getMaxBinders());
        i = randomInt();
        options.setMaxBinders(i);
        assertEquals(i, options.getMaxBinders());

        assertEquals(ServerOptions.DEFAULT_DOC_STREAM_BATCH_SIZE, options.getDocStreamBatchSize());
        i = randomInt();
        options.setDocStreamBatchSize(i);
        assertEquals(i, options.getDocStreamBatchSize());
    }

    @Test
    public void testServerOptionsFromEmptyJson() throws Exception {
        JsonObject json = new JsonObject();
        ServerOptions options = new ServerOptions(json);
        assertEquals(ServerOptions.DEFAULT_DOCS_DIR, options.getDocsDir());
        assertEquals(ServerOptions.DEFAULT_LOGS_DIR, options.getLogsDir());
        assertEquals(ServerOptions.DEFAULT_MAX_LOG_CHUNK_SIZE, options.getMaxLogChunkSize());
        assertEquals(ServerOptions.DEFAULT_MAX_RECORD_SIZE, options.getMaxRecordSize());
        assertEquals(ServerOptions.DEFAULT_PREALLOCATE_SIZE, options.getPreallocateSize());
        assertEquals(ServerOptions.DEFAULT_READ_BUFFER_SIZE, options.getReadBufferSize());
        assertEquals(ServerOptions.DEFAULT_QUERY_MAX_UNACKED_BYTES, options.getQueryMaxUnackedBytes());
        assertEquals(ServerOptions.DEFAULT_SUBSCRIPTION_MAX_UNACKED_BYTES, options.getSubscriptionMaxUnackedBytes());
        assertEquals(ServerOptions.DEFAULT_PROJECTION_MAX_UNACKED_EVENTS, options.getProjectionMaxUnackedEvents());
        assertEquals(ServerOptions.DEFAULT_LOG_FLUSH_INTERVAL, options.getLogFlushInterval());
        assertEquals(ServerOptions.DEFAULT_MAX_BINDER_SIZE, options.getMaxBinderSize());
        assertEquals(ServerOptions.DEFAULT_MAX_BINDERS, options.getMaxBinders());
        assertEquals(ServerOptions.DEFAULT_DOC_STREAM_BATCH_SIZE, options.getDocStreamBatchSize());
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
        ServerOptions options = new ServerOptions(json);
        assertEquals(docsDir, options.getDocsDir());
        assertEquals(logsDir, options.getLogsDir());
        assertEquals(maxLogChunkSize, options.getMaxLogChunkSize());
        assertEquals(maxRecordSize, options.getMaxRecordSize());
        assertEquals(preallocateSize, options.getPreallocateSize());
        assertEquals(readBufferSize, options.getReadBufferSize());

        assertEquals(queryMaxUnackedBytes, options.getQueryMaxUnackedBytes());
        assertEquals(subscriptionMaxUnackedBytes, options.getSubscriptionMaxUnackedBytes());
        assertEquals(projectionMaxUnackedEvents, options.getProjectionMaxUnackedEvents());
        assertEquals(logFlushInterval, options.getLogFlushInterval());
        assertEquals(maxBinderSize, options.getMaxBinderSize());
        assertEquals(maxBinders, options.getMaxBinders());

        assertEquals(nso, options.getNetServerOptions());
        assertEquals(hostName, options.getNetServerOptions().getHost());
    }
}

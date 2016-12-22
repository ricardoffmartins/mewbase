package com.tesco.mewbase;

import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetServerOptionsConverter;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class OptionsTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(OptionsTest.class);
    private static final int fsize = 4 * 1024;

    @Test
    public void testClientOptions() throws Exception {

        ClientOptions options = new ClientOptions();
        assertEquals(new NetClientOptions(), options.getNetClientOptions());
        assertEquals(ClientOptions.DEFAULT_PORT, options.getPort());
        assertEquals(ClientOptions.DEFAULT_HOST, options.getHost());

        NetClientOptions netClientOptions2 = new NetClientOptions();
        options.setNetClientOptions(netClientOptions2);
        assertSame(netClientOptions2, options.getNetClientOptions());

        options.setPort(1547);
        assertEquals(1547, options.getPort());

        options.setHost("somehost");
        assertEquals("somehost", options.getHost());
    }

    @Test
    public void testServerOptions() throws Exception {

        ServerOptions options = new ServerOptions();
        assertEquals(new NetServerOptions().setPort(ServerOptions.DEFAULT_PORT), options.getNetServerOptions());
        NetServerOptions netServerOptions2 = new NetServerOptions();
        options.setNetServerOptions(netServerOptions2);
        assertSame(netServerOptions2, options.getNetServerOptions());

        options.setDocsDir("foo");
        assertSame("foo", options.getDocsDir());

        options.setDocsDir("foo");
        assertSame("foo", options.getDocsDir());

        options.setMaxLogChunkSize(fsize);
        options.setMaxRecordSize(fsize);
        options.setPreallocateSize(fsize);
        options.setMaxRecordSize(fsize);

        assert (options.getMaxLogChunkSize() == fsize);
        assert (options.getPreallocateSize() == fsize);
        assert (options.getMaxRecordSize() == fsize);
        assert (options.getReadBufferSize() == fsize);
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
        assertEquals(new NetServerOptions(), options.getNetServerOptions());
    }

    @Test
    public void testServerOptionsFromJson() throws Exception {
        JsonObject json = new JsonObject();
        json.put("docsDir", "/testdocsdir");
        json.put("logsDir", "/testlogsdir");
        json.put("maxLogChunkSize", 12345);
        json.put("maxRecordSize", 1234);
        json.put("preallocateSize", 123456);
        json.put("readBufferSize", 321);
        NetServerOptions nso = new NetServerOptions().setHost("somehost");
        JsonObject jnso = new JsonObject();
        NetServerOptionsConverter.toJson(nso, jnso);
        json.put("netServerOptions", jnso);
        ServerOptions options = new ServerOptions(json);
        assertEquals("/testdocsdir", options.getDocsDir());
        assertEquals("/testlogsdir", options.getLogsDir());
        assertEquals(12345, options.getMaxLogChunkSize());
        assertEquals(1234, options.getMaxRecordSize());
        assertEquals(123456, options.getPreallocateSize());
        assertEquals(321, options.getReadBufferSize());

        assertEquals(nso, options.getNetServerOptions());
    }
}

package com.tesco.mewbase;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;

import java.io.File;

/**
 * Created by tim on 01/01/17.
 */
public class ServerTestBase extends MewbaseTestBase {

    protected static final String TEST_CHANNEL_1 = "channel1";
    protected static final String TEST_CHANNEL_2 = "channel2";

    protected static final String TEST_BINDER1 = "binder1";
    protected static final String TEST_BINDER2 = "binder2";

    protected Vertx vertx;
    protected Server server;
    protected Client client;
    protected File logsDir;
    protected File docsDir;

    @Before
    public void before(TestContext context) throws Exception {
        setup(context);
    }

    @After
    public void after(TestContext context) throws Exception {
        tearDown(context);
    }

    protected void setup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        setup0();
    }

    protected void tearDown(TestContext context) throws Exception {
        stopServerAndClient();
        AsyncResCF<Void> cf = new AsyncResCF<>();
        vertx.close(cf);
        cf.get();
    }

    protected void setup0() throws Exception {
        createDirectories();
        startServerAndClient();
        setupChannelsAndBinders();
    }

    protected void createDirectories() throws Exception {
        logsDir = testFolder.newFolder();
        docsDir = testFolder.newFolder();
    }

    protected void startServerAndClient() throws Exception {
        startServer();
        startClient();
    }

    protected void startServer() throws Exception {
        ServerOptions serverOptions = createServerOptions();
        server = Server.newServer(vertx, serverOptions);
        server.start().get();
    }

    protected void startClient() throws Exception {
        ClientOptions clientOptions = createClientOptions();
        client = Client.newClient(vertx, clientOptions);
    }

    protected void stopServerAndClient() throws Exception {
        if (client != null) {
            client.close().get();
        }
        if (server != null) {
            server.stop().get();
        }
    }

    protected void restart() throws Exception {
        boolean hasClient = client != null;
        stopServerAndClient();
        startServer();
        if (hasClient) {
            startClient();
        }
        setupChannelsAndBinders();
    }

    protected ServerOptions createServerOptions() {
        return new ServerOptions()
                .setLogsDir(logsDir.getPath())
                .setDocsDir(docsDir.getPath());
    }

    protected ClientOptions createClientOptions() {
        return new ClientOptions();
    }

    protected void setupChannelsAndBinders() throws Exception {
    }


}

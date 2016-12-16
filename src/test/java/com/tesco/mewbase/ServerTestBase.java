package com.tesco.mewbase;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.MewAdmin;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.unit.TestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by tim on 28/10/16.
 */
public class ServerTestBase extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(PubSubTest.class);

    protected Server server;
    protected Client client;
    protected File logDir;
    protected File docsDir;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        logDir = testFolder.newFolder();
        docsDir = testFolder.newFolder();
        ServerOptions serverOptions = createServerOptions(logDir);
        ClientOptions clientOptions = createClientOptions();
        server = Server.newServer(vertx, serverOptions);
        server.start().get();
        client = Client.newClient(vertx, clientOptions);
        setupChannelsAndBinders();
    }

    protected void setupChannelsAndBinders() throws Exception {
        MewAdmin admin = server.admin();
        admin.createChannel(TEST_CHANNEL_1).get();
        admin.createChannel(TEST_CHANNEL_2).get();
        admin.createBinder(TEST_BINDER1).get();
    }

    @Override
    protected void tearDown(TestContext context) throws Exception {
        client.close().get();
        server.stop().get();
        super.tearDown(context);
    }

    protected ServerOptions createServerOptions(File logDir) throws Exception {
        return new ServerOptions()
                .setLogsDir(logDir.getPath())
                .setDocsDir(docsDir.getPath());
    }

    protected ClientOptions createClientOptions() {
        return new ClientOptions().setNetClientOptions(new NetClientOptions());
    }
}

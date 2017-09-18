package io.mewbase;


import io.mewbase.server.Server;
import io.mewbase.server.MewbaseOptions;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;

import java.io.File;

/**
 * Created by tim on 01/01/17.
 */
public class ServerTestBase extends MewbaseTestBase {

    protected Vertx vertx;
    protected Server server;

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
        stopServer();
        AsyncResCF<Void> cf = new AsyncResCF<>();
        vertx.close(cf);
        cf.get();
    }

    protected void setup0() throws Exception {
        createDirectories();
        startServer();
    }

    protected void createDirectories() throws Exception {
        docsDir = testFolder.newFolder();
    }


    protected void startServer() throws Exception {
        MewbaseOptions mewbaseOptions = createServerOptions();
        server = Server.newServer(vertx, mewbaseOptions);
        server.start().get();
    }

    protected void stopServer() throws Exception {
        if ( server != null) {
             server.stop().get();
        }
    }

    protected void restart() throws Exception {
        stopServer();
        startServer();

    }

    protected MewbaseOptions createServerOptions() {
        return new MewbaseOptions().setDocsDir(docsDir.getPath());
    }

}
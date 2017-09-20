package io.mewbase;


import io.mewbase.server.Server;
import io.mewbase.server.MewbaseOptions;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;

/**
 * Created by tim on 01/01/17.
 */
public class ServerTestBase extends MewbaseTestBase {

    protected Vertx vertx;
    protected Server server;

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
        startServer();
    }

    protected void tearDown(TestContext context) throws Exception {
        stopServer();
        AsyncResCF<Void> cf = new AsyncResCF<>();
        vertx.close(cf);
        cf.get();
    }


    protected void startServer() throws Exception {
        MewbaseOptions mewbaseOptions = createMewbaseOptions();
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


}
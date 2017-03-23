package io.mewbase.cluster;

import io.mewbase.MewbaseTestBase;
import io.mewbase.client.MewException;
import io.mewbase.server.Server;
import io.mewbase.server.ServerOptions;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.unit.TestContext;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static io.mewbase.server.ServerOptions.DEFAULT_PORT;

/**
 * Created by tim on 22/03/17.
 */
public class ClusterTestBase extends MewbaseTestBase {

    protected ArrayList<Server> servers = new ArrayList<>();

    protected void startServers(int numServers) throws Exception {
        File dataDirBase = testFolder.newFolder();
        for (int i = 0; i < numServers; i++) {
            AsyncResCF<Vertx> ar = new AsyncResCF<>();
            ClusterManager clusterManager = getClusterManager();
            VertxOptions vertxOptions = new VertxOptions().setClustered(true).setClusterManager(clusterManager);
            Vertx.clusteredVertx(vertxOptions, ar);
            Vertx vertx = ar.get();
            ServerOptions options = new ServerOptions();
            File serverDir = new File(dataDirBase, "server-" + i);
            File logsDir = new File(serverDir, "logs");
            File docsDir = new File(serverDir, "docs");
            if (!logsDir.mkdirs()) {
                throw new IOException("Failed to create dirs");
            }
            if (!docsDir.mkdirs()) {
                throw new IOException("Failed to create dirs");
            }
            options.setLogsDir(logsDir.getPath());
            options.setDocsDir(docsDir.getPath());
            options.getNetServerOptions().setPort(DEFAULT_PORT + i);
            options.setRestServiceAdaptorEnabled(false);
            Server server = Server.newServer(vertx, clusterManager, options);
            server.start().get();
            setupChannelsAndBinders(server, i);
            servers.add(server);
        }
    }

    protected void setupChannelsAndBinders(Server server, int serverNumber) throws Exception {
    }

    protected void stopServers() throws Exception {
        for (Server server: servers) {
            server.stop().get();
        }
        servers.clear();
    }

    protected ClusterManager getClusterManager() {
        return new HazelcastClusterManager();
    }

    @Before
    public void before(TestContext context) throws Exception {
        setup(context);
    }

    @After
    public void after(TestContext context) throws Exception {
        tearDown(context);
    }

    protected void setup(TestContext context) throws Exception {

    }

    protected void tearDown(TestContext context) throws Exception {
       stopServers();
    }
}

package io.mewbase.cluster;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.Server;
import io.mewbase.server.cluster.TopologyManager;
import io.mewbase.server.impl.ServerImpl;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by tim on 22/03/17.
 */
@RunWith(VertxUnitRunner.class)
public class SimpleClusterTest extends ClusterTestBase {

    private final static Logger logger = LoggerFactory.getLogger(SimpleClusterTest.class);


    @Override
    protected void setupChannelsAndBinders(Server server, int serverNumber) throws Exception {
        server.createChannel("test-channel").get();
    }

    @Override
    protected void setup(TestContext context) throws Exception {
        startServers(2);
    }

    @Test
    public void testFoo() {
        assertEquals(2, servers.size());
        Map<String, BsonObject> map1 = servers.get(0).getClusterManager().getSyncMap(TopologyManager.MB_CLUSTER_MAP);
        logger.trace("***** MAP IS");
        for (Map.Entry<String, BsonObject> entry: map1.entrySet()) {
            logger.trace(entry.getKey() + ":" + entry.getValue());
        }
    }
}

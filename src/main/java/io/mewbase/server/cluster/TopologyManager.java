package io.mewbase.server.cluster;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;
import io.mewbase.server.impl.ServerImpl;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by tim on 22/03/17.
 */
public class TopologyManager implements NodeListener {

    private final static Logger logger = LoggerFactory.getLogger(TopologyManager.class);

    public static final String MB_CLUSTER_MAP = "_mb.clusterMap";

    private final Vertx vertx;
    private final ClusterManager clusterManager;
    private String nodeID;
    private Map<String, BsonObject> clusterMap;
    private BsonObject clusterInfo;

    public TopologyManager(Vertx vertx, ClusterManager clusterManager) {
        this.vertx = vertx;
        this.clusterManager = clusterManager;
        clusterInfo = new BsonObject();
    }

    public void start() {
        if (clusterManager == null) {
            return;
        }
        this.nodeID = clusterManager.getNodeID();
        clusterManager.nodeListener(this);
        clusterMap = clusterManager.getSyncMap(MB_CLUSTER_MAP);
    }

    public void stop() {
        if (clusterManager == null) {
            return;
        }
        clusterMap.remove(nodeID);
    }

    public synchronized void addChannel(String channelName) {
        if (clusterManager == null) {
            return;
        }
        BsonObject channels = clusterInfo.getBsonObject("channels");
        if (channels == null) {
            channels = new BsonObject();
            clusterInfo.put("channels", channels);
        }
        channels.put(channelName, new BsonObject());
        logger.trace(nodeID + " Added channel to cluster " + clusterInfo.encodeToString());
    }

    public synchronized void syncToCluster() {
        if (clusterManager == null) {
            return;
        }
        clusterMap.put(nodeID, clusterInfo);
    }

    @Override
    public void nodeAdded(String nodeID) {

    }

    @Override
    public void nodeLeft(String nodeID) {
        if (clusterMap.containsKey(nodeID)) {
            logger.trace("Node " + nodeID  + " crashed");
            // Remove the entry
            clusterMap.remove(nodeID);
        } else {
            // Clean close of node
        }
    }
}

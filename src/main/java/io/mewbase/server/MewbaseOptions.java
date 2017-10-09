package io.mewbase.server;

import io.mewbase.server.impl.auth.NoAuthAuthProvider;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServerOptions;

/**
 * Each key element of Mewbase can be configured at startup or dynamicly before starting that component
 *
 * By convention these parameter are set to make systems work together in the default case.
 *
 * Created by tim on 22/09/16.
 */
public class MewbaseOptions {

    //  **** DEFAULTS ****

    // Server Connections
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 7451;
    public static final int DEFAULT_CONNECTION_IDLE_TIMEOUT = 30;

    // Limit Event Size
    public static final int DEFAULT_MAX_RECORD_SIZE = 4 * 1024 * 1024;

    // Event Source
    public static final String DEFAULT_SOURCE_USERNAME = "TestClient";
    public static final String DEFAULT_SOURCE_CLUSTER_NAME = "test-cluster";
    public static final String DEFAULT_SOURCE_URL = "nats://localhost:4222";

    // Event Sink
    public static final String DEFAULT_SINK_USERNAME = "TestClient";
    public static final String DEFAULT_SINK_CLUSTER_NAME = "test-cluster";
    public static final String DEFAULT_SINK_URL = "nats://localhost:4222";

    // Binders
    public static final String DEFAULT_BINDERS_DIR = "mewdata/binders";
    public static final long DEFAULT_MAX_BINDER_SIZE = 1024L * 1024L * 1024L; //1 Gigabyte // * 1024L; // 1 Terabyte
    public static final int DEFAULT_MAX_BINDERS = 256;


    // **** VALUES ****

    // Server
    private NetServerOptions netServerOptions = new NetServerOptions().setPort(DEFAULT_PORT).setHost(DEFAULT_HOST)
            .setIdleTimeout(DEFAULT_CONNECTION_IDLE_TIMEOUT);

    private int maxRecordSize = DEFAULT_MAX_RECORD_SIZE;


    // Event Source
    private String sourceUserName = DEFAULT_SOURCE_USERNAME;
    public String sourceClusterName = DEFAULT_SOURCE_CLUSTER_NAME;
    public String sourceUrl = DEFAULT_SOURCE_URL;


    // Event Sink
    private String sinkUserName = DEFAULT_SINK_USERNAME;


    public String sinkClusterName = DEFAULT_SINK_CLUSTER_NAME;
    public String sinkUrl = DEFAULT_SINK_URL;


    // Binders
    private String docsDir = DEFAULT_BINDERS_DIR;
    private long maxBinderSize = DEFAULT_MAX_BINDER_SIZE;
    private int maxBinders = DEFAULT_MAX_BINDERS;

    /**
     * Apply all default values
     */
    public MewbaseOptions() {
    }

    public MewbaseOptions(JsonObject jsonObject) {

        JsonObject nso = jsonObject.getJsonObject("netServerOptions");
        this.netServerOptions = nso == null ? new NetServerOptions() : new NetServerOptions(nso);

        this.maxRecordSize = jsonObject.getInteger("maxRecordSize", DEFAULT_MAX_RECORD_SIZE);

        // Event Source
        this.sourceUserName = jsonObject.getString("sourceUserName", DEFAULT_SOURCE_USERNAME);
        this.sourceClusterName = jsonObject.getString("sourceClusterName",DEFAULT_SOURCE_CLUSTER_NAME);
        this.sourceUrl = jsonObject.getString("sourceUrl",DEFAULT_SOURCE_URL);

        // Event Sink
        this.sinkUserName = jsonObject.getString("sinkUserName", DEFAULT_SINK_USERNAME);
        this.sinkClusterName = jsonObject.getString("sinkClusterName",DEFAULT_SINK_CLUSTER_NAME);
        this.sinkUrl = jsonObject.getString("sinkUrl",DEFAULT_SINK_URL);

        // Binders
        this.docsDir = jsonObject.getString("docsDir", DEFAULT_BINDERS_DIR);
        this.maxBinderSize = jsonObject.getLong("maxBinderSize", DEFAULT_MAX_BINDER_SIZE);
        this.maxBinders = jsonObject.getInteger("maxBinders", DEFAULT_MAX_BINDERS);
    }


    // Server options
    public NetServerOptions getNetServerOptions() {
        return netServerOptions;
    }

    public MewbaseOptions setNetServerOptions(NetServerOptions netServerOptions) {
        this.netServerOptions = netServerOptions;
        return this;
    }



    // Event Source
    public String getSourceUserName() {
        return sourceUserName;
    }

    public MewbaseOptions setSourceUserName(String sourceUserName) {
        this.sourceUserName = sourceUserName;
        return this;
    }

    public String getSourceClusterName() {
        return sourceClusterName;
    }

    public MewbaseOptions setSourceClusterName(String sourceClusterName) {
        this.sourceClusterName = sourceClusterName;
        return this;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public MewbaseOptions setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
        return this;
    }



    // Event Sink
    public String getSinkUserName() {
        return sinkUserName;
    }

    public MewbaseOptions setSinkUserName(String sinkUserName) {
        this.sinkUserName = sinkUserName;
        return this;
    }

    public String getSinkClusterName() {
        return sinkClusterName;
    }

    public MewbaseOptions setSinkClusterName(String sinkClusterName) {
        this.sinkClusterName = sinkClusterName;
        return this;
    }

    public String getSinkUrl() {
        return sinkUrl;
    }

    public MewbaseOptions setSinkUrl(String sinkUrl) {
        this.sinkUrl = sinkUrl;
        return this;
    }


    // Binders
    public String getDocsDir() {
        return docsDir;
    }

    public MewbaseOptions setDocsDir(String docsDir) {
        this.docsDir = docsDir;
        return this;
    }

    public long getMaxBinderSize() {
        return maxBinderSize;
    }

    public MewbaseOptions setMaxBinderSize(long maxBinderSize) {
        this.maxBinderSize = maxBinderSize;
        return this;
    }

    public int getMaxBinders() {
        return maxBinders;
    }

    public MewbaseOptions setMaxBinders(int maxBinders) {
        this.maxBinders = maxBinders;
        return this;
    }




}

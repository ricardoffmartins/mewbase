package io.mewbase.server;

import io.mewbase.server.impl.auth.NoAuthAuthProvider;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServerOptions;

/**
 * Created by tim on 22/09/16.
 */
public class ServerOptions {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 7451;
    public static final String DEFAULT_DOCS_DIR = "mewdata/docs";
    public static final String DEFAULT_LOGS_DIR = "mewdata/eventlogs";
    public static final int DEFAULT_MAX_LOG_CHUNK_SIZE = 4 * 10 * 1024 * 1024;
    public static final int DEFAULT_PREALLOCATE_SIZE = 0;
    public static final int DEFAULT_MAX_RECORD_SIZE = 4 * 1024 * 1024;
    public static final int DEFAULT_READ_BUFFER_SIZE = 4 * 1024;
    public static final int DEFAULT_QUERY_MAX_UNACKED_BYTES = 4 * 1024 * 1024;
    public static final int DEFAULT_SUBSCRIPTION_MAX_UNACKED_BYTES = 4 * 1024 * 1024;
    public static final int DEFAULT_PROJECTION_MAX_UNACKED_EVENTS = 1000;
    public static final long DEFAULT_LOG_FLUSH_INTERVAL = 10 * 1000;
    public static final long DEFAULT_MAX_BINDER_SIZE = 1024L * 1024L * 1024L * 1024L; // 1 Terabyte
    public static final int DEFAULT_MAX_BINDERS = 128;
    public static final int DEFAULT_DOC_STREAM_BATCH_SIZE = 1000;
    public static final int DEFAULT_CONNECTION_IDLE_TIMEOUT = 30;

    private NetServerOptions netServerOptions = new NetServerOptions().setPort(DEFAULT_PORT).setHost(DEFAULT_HOST)
            .setIdleTimeout(DEFAULT_CONNECTION_IDLE_TIMEOUT);
    private String docsDir = DEFAULT_DOCS_DIR;
    private MewbaseAuthProvider authProvider = new NoAuthAuthProvider();

    // no longer logs
    // private String logsDir = DEFAULT_LOGS_DIR;
    // private int maxLogChunkSize = DEFAULT_MAX_LOG_CHUNK_SIZE;
    // private int preallocateSize = DEFAULT_PREALLOCATE_SIZE;
    private int maxRecordSize = DEFAULT_MAX_RECORD_SIZE;
    // private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;
    private int queryMaxUnackedBytes = DEFAULT_QUERY_MAX_UNACKED_BYTES;
    private int subscriptionMaxUnackedBytes = DEFAULT_SUBSCRIPTION_MAX_UNACKED_BYTES;
    private int projectionMaxUnackedEvents = DEFAULT_PROJECTION_MAX_UNACKED_EVENTS;
    // private long logFlushInterval = DEFAULT_LOG_FLUSH_INTERVAL;


    private long maxBinderSize = DEFAULT_MAX_BINDER_SIZE;
    private int maxBinders = DEFAULT_MAX_BINDERS;
    private int docStreamBatchSize = DEFAULT_DOC_STREAM_BATCH_SIZE;

    public ServerOptions() {
    }

    public ServerOptions(JsonObject jsonObject) {
        JsonObject nso = jsonObject.getJsonObject("netServerOptions");
        this.netServerOptions = nso == null ? new NetServerOptions() : new NetServerOptions(nso);
        this.docsDir = jsonObject.getString("docsDir", DEFAULT_DOCS_DIR);
        this.maxRecordSize = jsonObject.getInteger("maxRecordSize", DEFAULT_MAX_RECORD_SIZE);

        this.queryMaxUnackedBytes = jsonObject.getInteger("queryMaxUnackedBytes", DEFAULT_QUERY_MAX_UNACKED_BYTES);
        this.subscriptionMaxUnackedBytes = jsonObject.getInteger("subscriptionMaxUnackedBytes", DEFAULT_SUBSCRIPTION_MAX_UNACKED_BYTES);
        this.projectionMaxUnackedEvents = jsonObject.getInteger("projectionMaxUnackedEvents", DEFAULT_PROJECTION_MAX_UNACKED_EVENTS);

        this.maxBinderSize = jsonObject.getLong("maxBinderSize", DEFAULT_MAX_BINDER_SIZE);
        this.maxBinders = jsonObject.getInteger("maxBinders", DEFAULT_MAX_BINDERS);
        this.docStreamBatchSize = jsonObject.getInteger("docStreamBatchSize", DEFAULT_DOC_STREAM_BATCH_SIZE);
    }

    public NetServerOptions getNetServerOptions() {
        return netServerOptions;
    }

    public ServerOptions setNetServerOptions(NetServerOptions netServerOptions) {
        this.netServerOptions = netServerOptions;
        return this;
    }

    public String getDocsDir() {
        return docsDir;
    }

    public ServerOptions setDocsDir(String docsDir) {
        this.docsDir = docsDir;
        return this;
    }

    public MewbaseAuthProvider getAuthProvider() {
        return authProvider;
    }

    public ServerOptions setAuthProvider(MewbaseAuthProvider authProvider) {
        this.authProvider = authProvider;
        return this;
    }


    public int getQueryMaxUnackedBytes() {
        return queryMaxUnackedBytes;
    }

    public ServerOptions setQueryMaxUnackedBytes(int queryMaxUnackedBytes) {
        this.queryMaxUnackedBytes = queryMaxUnackedBytes;
        return this;
    }

    public int getSubscriptionMaxUnackedBytes() {
        return subscriptionMaxUnackedBytes;
    }

    public ServerOptions setSubscriptionMaxUnackedBytes(int subscriptionMaxUnackedBytes) {
        this.subscriptionMaxUnackedBytes = subscriptionMaxUnackedBytes;
        return this;
    }

    public int getProjectionMaxUnackedEvents() {
        return projectionMaxUnackedEvents;
    }

    public ServerOptions setProjectionMaxUnackedEvents(int projectionMaxUnackedEvents) {
        this.projectionMaxUnackedEvents = projectionMaxUnackedEvents;
        return this;
    }


    public long getMaxBinderSize() {
        return maxBinderSize;
    }

    public ServerOptions setMaxBinderSize(long maxBinderSize) {
        this.maxBinderSize = maxBinderSize;
        return this;
    }

    public int getMaxBinders() {
        return maxBinders;
    }

    public ServerOptions setMaxBinders(int maxBinders) {
        this.maxBinders = maxBinders;
        return this;
    }

    public int getDocStreamBatchSize() {
        return docStreamBatchSize;
    }

    public ServerOptions setDocStreamBatchSize(int docStreamBatchSize) {
        this.docStreamBatchSize = docStreamBatchSize;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerOptions options = (ServerOptions)o;

        if (maxRecordSize != options.maxRecordSize) return false;
        if (queryMaxUnackedBytes != options.queryMaxUnackedBytes) return false;
        if (subscriptionMaxUnackedBytes != options.subscriptionMaxUnackedBytes) return false;
        if (projectionMaxUnackedEvents != options.projectionMaxUnackedEvents) return false;
        if (maxBinderSize != options.maxBinderSize) return false;
        if (maxBinders != options.maxBinders) return false;
        if (docStreamBatchSize != options.docStreamBatchSize) return false;
        if (netServerOptions != null ? !netServerOptions.equals(options.netServerOptions) : options.netServerOptions != null)
            return false;
        if (docsDir != null ? !docsDir.equals(options.docsDir) : options.docsDir != null) return false;
        if (authProvider != null ? !authProvider.equals(options.authProvider) : options.authProvider != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = netServerOptions != null ? netServerOptions.hashCode() : 0;
        result = 31 * result + (docsDir != null ? docsDir.hashCode() : 0);
        result = 31 * result + (authProvider != null ? authProvider.hashCode() : 0);
        result = 31 * result + maxRecordSize;
        result = 31 * result + queryMaxUnackedBytes;
        result = 31 * result + subscriptionMaxUnackedBytes;
        result = 31 * result + projectionMaxUnackedEvents;
        result = 31 * result + maxBinders;
        result = 31 * result + docStreamBatchSize;
        return result;
    }
}

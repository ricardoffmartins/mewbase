package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tim on 07/10/16.
 */
public class FileLogManager implements LogManager {

    private final static Logger logger = LoggerFactory.getLogger(FileLogManager.class);

    private final Vertx vertx;
    private final FileAccess faf;
    private final File logsDir;
    private final Map<String, FileLog> logs = new ConcurrentHashMap<>();
    private final ServerOptions options;

    public FileLogManager(Vertx vertx, ServerOptions options, FileAccess faf) {
        this.vertx = vertx;
        this.logsDir = new File(options.getLogsDir());
        if (!logsDir.exists()) {
            if (!logsDir.mkdirs()) {
                throw new MewException("Failed to create directory " + options.getLogsDir());
            }
        }
        this.options = options;
        this.faf = faf;
    }

    @Override
    public synchronized CompletableFuture<Boolean> createLog(String channel) {
        if (!logs.containsKey(channel)) {
            File logDir = new File(logsDir, channel);
            boolean newLog = !logDir.exists();
            FileLog log = new FileLog(vertx, faf, options, channel);
            logs.put(channel, log);
            return log.start().thenApply(v -> newLog);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture[] arr = new CompletableFuture[logs.size()];
        int i = 0;
        for (Log log : logs.values()) {
            arr[i++] = log.close();
        }
        return CompletableFuture.allOf(arr);
    }

    @Override
    public Log getLog(String channel) {
        return logs.get(channel);
    }
}

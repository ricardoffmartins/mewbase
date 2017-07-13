package io.mewbase.util;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SizedExecutorPool {

    private final String name;
    private final int size;
    private final List<WorkerExecutor> pool;
    private final Vertx vertx;

    private AtomicInteger accessCounter = new AtomicInteger(0);

    public SizedExecutorPool(Vertx vertx, String poolName, int poolSize) {
        if (poolSize < 1) throw new AssertionError("Executor pool size must be at least 1");
        this.vertx = vertx;
        name = poolName;
        size = poolSize;
        pool = new ArrayList<>(poolSize);
    }

    public synchronized WorkerExecutor getWorkerExecutor() {
        if (accessCounter.get() < size) pool.add(vertx.createSharedWorkerExecutor(name, 1));
        return pool.get(accessCounter.getAndIncrement() % size);
    }

}
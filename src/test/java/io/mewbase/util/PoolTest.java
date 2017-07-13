/*
 *
 *  Copyright 2014 Red Hat, Inc.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 *
 *
 */

package io.mewbase.util;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertNotSame;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;

/**
 * @author Nige
 */
@RunWith(VertxUnitRunner.class)
public class PoolTest {

    private final Vertx vertx = Vertx.vertx();
    private final String POOL_NAME = "TestPool";

    @Test
    public void failsForNonsensicalPoolSize() throws Exception {
        try {
            SizedExecutorPool pool = new SizedExecutorPool(vertx, POOL_NAME, 0);
            fail("Should not be able to create pool with size less then 1");
        } catch (AssertionError error){
        }
    }


    @Test
    public void poolCreatesSingleWorker() throws Exception {
        final int size = 1;
        SizedExecutorPool pool = new SizedExecutorPool(vertx,POOL_NAME,size);
        WorkerExecutor exec1 = pool.getWorkerExecutor();
        assertNotNull(exec1);
        WorkerExecutor exec2 = pool.getWorkerExecutor();
        assertEquals(exec1,exec2);
    }

    @Test
    public void poolCreatesManyWorkers() throws Exception {
        final int size = 3;
        SizedExecutorPool pool = new SizedExecutorPool(vertx,POOL_NAME,size);
        WorkerExecutor exec1 = pool.getWorkerExecutor();
        assertNotNull(exec1);
        WorkerExecutor exec2 = pool.getWorkerExecutor();
        assertNotNull(exec2);
        WorkerExecutor exec3 = pool.getWorkerExecutor();
        assertNotNull(exec3);

        assertNotSame(exec1,exec2);
        assertNotSame(exec2,exec3);
        assertNotSame(exec3,exec1);

        // check we have wrapped around
        WorkerExecutor exec4 = pool.getWorkerExecutor();
        assertEquals(exec1,exec4);

        // check others wrap
        WorkerExecutor exec5 = pool.getWorkerExecutor();
        assertEquals(exec2,exec5);
        WorkerExecutor exec6 = pool.getWorkerExecutor();
        assertEquals(exec3,exec6);

        // double wrap
        WorkerExecutor exec7 = pool.getWorkerExecutor();
        assertEquals(exec7,exec4); // and 4 ==1 already
    }
}

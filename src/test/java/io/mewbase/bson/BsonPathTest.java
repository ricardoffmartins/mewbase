/*
 * Copyright (c) 2011-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Derived from original file JsonObjectTest.java from Vert.x
 */

package io.mewbase.bson;

import io.mewbase.TestUtils;
import io.mewbase.client.MewException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static org.junit.Assert.*;

/**
 * @author Nige
 */
public class BsonPathTest {

    protected BsonObject bsonObject;

    @Before
    public void setUp() throws Exception {
        bsonObject = new BsonObject();
    }

    @Test
    public void testGoodPath() {

        final String car = "car";
        final String house = "house";
        final String dog = "dog";

        final String pathStr = car + "." + house + "." + dog;
        final Path path = new Path(pathStr);

        assertEquals( path.first().getKey(), car );
        assertEquals( path.last().getKey(), dog);

        assertEquals( path.getElems()[0].getKey(), car );
        assertEquals( path.getElems()[1].getKey(), house);
        assertEquals( path.getElems()[2].getKey(), dog );
    }

    @Test
    public void testSet() {
        final String pathStr = ".house";
        BsonObject bs = BsonPath.set(bsonObject, new Path(pathStr),123);
        assertEquals( Integer.valueOf(123), bs.getInteger(pathStr) );

//        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
//        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", null));
//        bsonObject.put("bar", "hello");
//        try {
//            bsonObject.getInteger("bar", 123);
//            fail();
//        } catch (ClassCastException e) {
//            // Ok
//        }
//        // Put as different Number types
//        bsonObject.put("foo", 123l);
//        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
//        bsonObject.put("foo", 123d);
//        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
//        bsonObject.put("foo", 123f);
//        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
//        bsonObject.put("foo", Long.MAX_VALUE);
//        assertEquals(Integer.valueOf(-1), bsonObject.getInteger("foo", 321));

//        // Null and absent values
//        bsonObject.putNull("foo");
//        assertNull(bsonObject.getInteger("foo", 321));
//        assertEquals(Integer.valueOf(321), bsonObject.getInteger("absent", 321));
//        assertNull(bsonObject.getInteger("foo", null));
//        assertNull(bsonObject.getInteger("absent", null));
//
//        try {
//            bsonObject.getInteger(null, null);
//            fail();
//        } catch (NullPointerException e) {
//            // OK
//        }
//
//    }


    }
}



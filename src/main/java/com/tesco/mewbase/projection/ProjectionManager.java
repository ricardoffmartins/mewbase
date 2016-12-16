package com.tesco.mewbase.projection;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public interface ProjectionManager {

    ProjectionBuilder buildProjection(String name);
}

package com.tesco.mewbase.projection;

import java.util.Set;

/**
 * Created by tim on 30/09/16.
 */
public interface ProjectionManager {

    ProjectionBuilder buildProjection(String name);

    Set<String> getProjectionNames();

    Projection getProjection(String projectionName);
}

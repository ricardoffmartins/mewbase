package io.mewbase.projection;

import java.util.List;

/**
 * Created by tim on 30/09/16.
 */
public interface ProjectionManager {

    ProjectionBuilder buildProjection(String name);

    List<String> listProjectionNames();

    Projection getProjection(String projectionName);
}

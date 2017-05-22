package io.mewbase.bson;

import com.sun.tools.javac.util.List;

import java.util.Arrays;

/**
 * Created by nigel on 19/05/2017.
 *
 * A path determines the value, array or object to extract or modify from a given bson
 * encoding e.g. To get the 2nd bolt, 3rd wheel on a car
 *
 * car.wheel[3].bolt[2]
 *
 */
public class Path {
    private PathElement[] elems;

    public Path(String path) {

        list = Arrays.stream(path.trim().split(".")).map( f -> f )


    }

}

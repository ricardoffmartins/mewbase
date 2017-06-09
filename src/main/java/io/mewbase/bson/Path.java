package io.mewbase.bson;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by nigel on 19/05/2017.
 *
 * A path determines the value, array or object to extract or modify from a given bson
 * encoding e.g. To get the 2nd bolt, 3rd wheel of a car given
 *
 * {
 *     car : {
 *         wheel : [
 *             bolt : [
 *
 *
 *     }
 * }
 *
 * .car.wheel[3].bolt[2]
 *
 */

public class Path {

    private PathElement [] elems;

    public Path(String path) {
        Stream<String> strElems = Arrays.stream(path.trim().split("\\."));
        Stream<PathElement> pathElems =  strElems.map( elem -> new PathElement(elem.trim()));
        elems = pathElems.toArray(PathElement[]::new);
        // which is ... in old money
        // String [] elemsStrs = path.trim().split("\\.");
        // elems = new PathElement[elemsStrs.length];
        // for (int i = 0; i  < elemsStrs.length; ++i) elems[i] = new PathElement(elemsStrs[i]);
    }


    public PathElement first() {
        return elems[0];
    }


    public PathElement last() {
        return elems[elems.length - 1];
    }


    public PathElement[] getElems() {
        return elems;
    }

}

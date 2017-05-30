package io.mewbase.bson;

/**
 * Created by nigel on 19/05/2017.
 */
public class PathElement {

    private String key;

    private long binOffset = 0;

    public PathElement(String key) {
        setKey(key);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}

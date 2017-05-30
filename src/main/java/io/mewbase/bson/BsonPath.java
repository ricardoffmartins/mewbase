package io.mewbase.bson;

/**
 * Created by tim on 04/11/16.
 */
public class BsonPath {

    public static BsonObject set(BsonObject doc, Path path, Object value) {
        BsonObject root = getRoot(doc, path);
        root.put(path.last().getKey(), value);
        return doc;
    }

    public static BsonObject add(BsonObject doc, Path path, int value ) {
        BsonObject root = getRoot(doc, path);
        String key = path.last().getKey();
        Object prevVal = root.getValue(key);
        Object newVal;
        if (prevVal != null) {
            if (prevVal instanceof Integer) {
                newVal = ((Integer)prevVal) + value;
            } else if (prevVal instanceof Long) {
                newVal = ((Long)prevVal) + value;
            } else if (prevVal instanceof Short) {
                newVal = ((Short)prevVal) + value;
            } else if (prevVal instanceof Byte) {
                newVal = ((Byte)prevVal) + value;
            } else {
                throw new IllegalArgumentException("Cannot increment " + prevVal);
            }
        } else {
            newVal = value;
        }
        root.put(key, newVal);
        return doc;
    }


    public static BsonObject inc(BsonObject doc, Path path) { return add(doc, path, 1); }

    // TODO support BsonArray
    private static BsonObject getRoot(BsonObject doc, Path path) {
        BsonObject rootObj = doc;
        for ( PathElement elem : path.getElems() ) {
            String key = elem.getKey();
            BsonObject child = rootObj.getBsonObject(key);
            if (child == null) {
                child = new BsonObject();
                rootObj.put(key, child);
            }
            rootObj = child;
        }
        return rootObj;
    }

}

package io.mewbase.server.filter;


import io.mewbase.bson.BsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;


public class FilterFactory {

    private final static Logger logger = LoggerFactory.getLogger(FilterFactory.class);

    private final static ConcurrentHashMap<String,Predicate<BsonObject>> filters = new ConcurrentHashMap<String,Predicate<BsonObject>>();

    public static Predicate<BsonObject> makeFilter(String name) {

       Predicate<BsonObject> filter = filters.get(name);
       if (filter != null) {
           logger.info("Found filter named '"+name+"' - applying to subscription");
        } else {
           logger.info("No filter found for name '" + name + " creating inclusion function filter");
           filter = bson -> true;
       }
       return filter;
    }

    public static void addFilter(String name, Predicate<BsonObject> filter) {
        filters.put(name,filter);
    }
}



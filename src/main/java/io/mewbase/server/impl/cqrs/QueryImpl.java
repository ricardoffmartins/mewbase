package io.mewbase.server.impl.cqrs;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.Binder;
import io.mewbase.server.Query;
import io.mewbase.server.QueryContext;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 10/01/17.
 */
public class QueryImpl implements Query {

    private final String name;
    private String binderName;
    private Binder binder;

    private BiFunction<BsonObject, QueryContext, Boolean> documentFilter;
    private Function<BsonObject, String> idSelector;

    public QueryImpl(String name) {
        this.name = name;
    }

    public String getBinderName() {
        return binderName;
    }

    public void setBinderName(String binderName) {
        this.binderName = binderName;
    }

    public BiFunction<BsonObject, QueryContext, Boolean> getDocumentFilter() {
        return documentFilter;
    }

    public void setDocumentFilter(BiFunction<BsonObject, QueryContext, Boolean> documentFilter) {
        this.documentFilter = documentFilter;
    }

    public String getName() {
        return name;
    }

    public Function<BsonObject, String> getIdSelector() {
        return idSelector;
    }

    public void setIdSelector(Function<BsonObject, String> idSelector) {
        this.idSelector = idSelector;
    }

    public Binder getBinder() {
        return binder;
    }

    public void setBinder(Binder binder) {
        this.binder = binder;
    }

}

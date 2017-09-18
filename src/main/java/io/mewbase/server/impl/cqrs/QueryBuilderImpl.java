package io.mewbase.server.impl.cqrs;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.Query;
import io.mewbase.server.QueryBuilder;
import io.mewbase.server.QueryContext;
import io.mewbase.server.impl.ServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

/**
 * Created by tim on 10/01/17.
 */
public class QueryBuilderImpl implements QueryBuilder {

    private final static Logger logger = LoggerFactory.getLogger(QueryBuilderImpl.class);

    private final CQRSManager cqrsManager;
    private final QueryImpl query;

    public QueryBuilderImpl(CQRSManager cqrsManager, String name) {
        this.cqrsManager = cqrsManager;
        this.query = new QueryImpl(name);
    }

    @Override
    public QueryBuilder from(String binderName) {
        query.setBinderName(binderName);
        return this;
    }

    @Override
    public QueryBuilder documentFilter(BiFunction<BsonObject, QueryContext, Boolean> documentFilter) {
        query.setDocumentFilter(documentFilter);
        return this;
    }

    @Override
    public Query create() {
        if (query.getBinderName() == null) {
            throw new IllegalStateException("Please specify a binder name");
        }
        if (query.getDocumentFilter() == null && query.getIdSelector() == null) {
            throw new IllegalStateException("Please specify either a document filter or id selector");
        }
        if (query.getDocumentFilter() != null && query.getIdSelector() != null) {
            throw new IllegalStateException("Can't set both document filter and id selector");
        }
        try {
            cqrsManager.registerQuery(query);
        } catch (Exception e) {
            logger.error("Failed to register query");
        }
        return query;
    }
}

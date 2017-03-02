package io.mewbase.server.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.impl.cqrs.QueryImpl;
import io.vertx.core.buffer.Buffer;

/**
 * Created by tim on 12/01/17.
 */
public class ConnectionQueryExecution extends QueryExecution {

    private final ConnectionImpl connection;
    private final int queryID;

    public ConnectionQueryExecution(ConnectionImpl connection, int queryID, QueryImpl query,
                                    BsonObject params) {
        super(query, params);
        this.connection = connection;
        this.queryID = queryID;
    }

    @Override
    protected Buffer writeQueryResult(BsonObject doc, boolean last) {
        return connection.writeQueryResult(doc, queryID, last);
    }

    @Override
    public void close() {
        super.close();
        connection.removeQueryState(queryID);
    }
}

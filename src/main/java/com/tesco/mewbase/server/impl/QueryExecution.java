package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.doc.DocReadStream;
import com.tesco.mewbase.query.QueryContext;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * Created by tim on 17/11/16.
 */
public class QueryExecution implements QueryContext {

    private final static Logger logger = LoggerFactory.getLogger(QueryExecution.class);

    private static final int MAX_UNACKED_BYTES = 4 * 1024 * 1024; // TODO make configurable

    private final ConnectionImpl connection;
    private final int queryID;
    private final DocReadStream readStream;
    private final BsonObject params;
    private final BiConsumer<QueryContext, BsonObject> queryConsumer;
    private final BsonObject state;
    private int unackedBytes;

    public QueryExecution(ConnectionImpl connection, int queryID, DocReadStream readStream,
                          BsonObject params,
                          BiConsumer<QueryContext, BsonObject> queryConsumer) {
        this.connection = connection;
        this.queryID = queryID;
        this.readStream = readStream;
        this.params = params;
        this.queryConsumer = queryConsumer;
        this.state = new BsonObject();
        readStream.handler(this::handleDoc);
    }

    public void close() {
        readStream.close();
    }

    @Override
    public void emitRow(BsonObject doc) {
        connection.checkContext();
        boolean last = !readStream.hasMore();
        Buffer buff = connection.writeQueryResult(doc, queryID, last);
        unackedBytes += buff.length();
        if (unackedBytes > MAX_UNACKED_BYTES) {
            readStream.pause();
        }
        if (last) {
            connection.removeQueryState(queryID);
        }
    }


    @Override
    public BsonObject params() {
        return params;
    }

    @Override
    public BsonObject state() {
        return state;
    }

    void handleAck(int bytes) {
        connection.checkContext();
        unackedBytes -= bytes;
        // Low watermark to prevent thrashing
        if (unackedBytes < MAX_UNACKED_BYTES / 2) {
            readStream.resume();
        }
    }

    private void handleDoc(BsonObject doc) {
        queryConsumer.accept(this, doc);
    }


}
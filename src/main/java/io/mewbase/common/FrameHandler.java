package io.mewbase.common;


import io.mewbase.bson.BsonObject;


/**
 * Created by tim on 23/09/16.
 */
public interface FrameHandler {

    void handleConnect(BsonObject frame);

    void handleResponse(BsonObject frame);

    void handlePublish(BsonObject frame);

    void handleStartTx(BsonObject frame);

    void handleCommitTx(BsonObject frame);

    void handleAbortTx(BsonObject frame);

    void handleSubscribe(BsonObject frame);

    void handleSubClose(BsonObject frame);

    void handleUnsubscribe(BsonObject frame);

    void handleSubResponse(BsonObject frame);

    void handleRecev(int size, BsonObject frame);

    void handleAckEv(BsonObject frame);

    void handleFindByID(BsonObject frame);

    void handleQuery(BsonObject frame);

    void handleQueryResult(int size, BsonObject frame);

    void handleQueryAck(BsonObject frame);

    void handlePing(BsonObject frame);


    void handleListBinders(BsonObject frame);

    void handleCreateBinder(BsonObject frame);

    void handleListChannels(BsonObject frame);

    void handleCreateChannel(BsonObject frame);

    void handleCommand(BsonObject frame);


}

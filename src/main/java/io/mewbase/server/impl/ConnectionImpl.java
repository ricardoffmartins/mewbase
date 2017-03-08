package io.mewbase.server.impl;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.mewbase.client.Client;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.Binder;
import io.mewbase.server.Log;
import io.mewbase.server.MewbaseAuthProvider;
import io.mewbase.server.MewbaseUser;
import io.mewbase.server.impl.auth.UnauthorizedUser;
import io.mewbase.server.impl.cqrs.QueryImpl;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 23/09/16.
 */
public class ConnectionImpl implements ServerFrameHandler {

    private final static Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

    private final ServerImpl server;
    private final TransportConnection transportConnection;
    private final Context context;
    private final Map<Integer, SubscriptionImpl> subscriptionMap = new HashMap<>();
    private final Map<Integer, QueryExecution> queryStates = new HashMap<>();

    private boolean closed;
    private MewbaseAuthProvider authProvider;
    private MewbaseUser user;
    private int subSeq;

    public ConnectionImpl(ServerImpl server, TransportConnection transportConnection, Context context,
                          MewbaseAuthProvider authProvider) {
        Protocol protocol = new Protocol(this);
        RecordParser recordParser = protocol.recordParser();
        transportConnection.handler(recordParser::handle);
        this.server = server;
        this.transportConnection = transportConnection;
        this.context = context;
        this.authProvider = authProvider;
        transportConnection.closeHandler(this::close);
    }

    @Override
    public void handleConnect(BsonObject frame) {
        checkContext();

        // TODO version checking

        BsonObject value = (BsonObject)frame.getValue(Protocol.CONNECT_AUTH_INFO);
        CompletableFuture<MewbaseUser> cf = authProvider.authenticate(value);

        cf.whenComplete((result, ex) -> {

            checkContext();
            BsonObject response = new BsonObject();
            if (ex != null) {
                sendConnectErrorResponse(Client.ERR_AUTHENTICATION_FAILED, "Authentication failed");
                logAndClose(ex.getMessage());
            } else {
                if (result != null) {
                    user = result;
                    response.put(Protocol.RESPONSE_OK, true);
                    writeResponse(Protocol.RESPONSE_FRAME, response);
                } else {
                    String nullUserMsg = "AuthProvider returned a null user";
                    logAndClose(nullUserMsg);
                    throw new IllegalStateException(nullUserMsg);
                }
            }
        });

    }

    @Override
    public void handlePublish(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.PUBLISH_FRAME, frame, () -> {
            String channel = frame.getString(Protocol.PUBLISH_CHANNEL);
            BsonObject event = frame.getBsonObject(Protocol.PUBLISH_EVENT);
            Integer sessID = frame.getInteger(Protocol.PUBLISH_SESSID);
            Integer requestID = frame.getInteger(Protocol.REQUEST_REQUEST_ID);

            if (channel == null) {
                missingField(Protocol.PUBLISH_CHANNEL, Protocol.PUBLISH_FRAME);
                return;
            }
            if (event == null) {
                missingField(Protocol.PUBLISH_EVENT, Protocol.PUBLISH_FRAME);
                return;
            }
            if (requestID == null) {
                missingField(Protocol.REQUEST_REQUEST_ID, Protocol.PUBLISH_FRAME);
                return;
            }
            Log log = server.getLog(channel);
            if (log == null) {
                sendErrorResponse(Client.ERR_NO_SUCH_CHANNEL, "no such channel " + channel, frame);
                return;
            }
            CompletableFuture<Long> cf = server.publishEvent(log, event);

            cf.handle((v, ex) -> {
                if (ex == null) {
                    writeResponseOK(frame);
                } else {
                    sendErrorResponse(Client.ERR_SERVER_ERROR, "failed to persist", frame);
                }
                return null;
            });
        });
    }

    @Override
    public void handleStartTx(BsonObject frame) {
        checkContext();

        throw new UnsupportedOperationException();
    }

    @Override
    public void handleCommitTx(BsonObject frame) {
        checkContext();

        throw new UnsupportedOperationException();
    }

    @Override
    public void handleAbortTx(BsonObject frame) {
        checkContext();

        throw new UnsupportedOperationException();
    }

    @Override
    public void handleSubscribe(BsonObject frame) {
        checkContext();
        authoriseThenHandle(Protocol.SUBSCRIBE_FRAME, frame, () -> {
            String channel = frame.getString(Protocol.SUBSCRIBE_CHANNEL);

            if (channel == null) {
                missingField(Protocol.SUBSCRIBE_CHANNEL, Protocol.SUBSCRIBE_FRAME);
            }

            Long startSeq = frame.getLong(Protocol.SUBSCRIBE_STARTPOS);
            Long startTimestamp = frame.getLong(Protocol.SUBSCRIBE_STARTTIMESTAMP);
            String durableID = frame.getString(Protocol.SUBSCRIBE_DURABLEID);
            BsonObject matcher = frame.getBsonObject(Protocol.SUBSCRIBE_MATCHER);
            SubDescriptor subDescriptor = new SubDescriptor().setStartPos(startSeq == null ? -1 : startSeq).setStartTimestamp(startTimestamp)
                    .setMatcher(matcher).setDurableID(durableID).setChannel(channel);
            int subID = subSeq++;
            checkWrap(subSeq);
            Log log = server.getLog(channel);
            if (log == null) {
                sendErrorResponse(Client.ERR_NO_SUCH_CHANNEL, "no such channel " + channel, frame);
            }
            SubscriptionImpl subscription = new SubscriptionImpl(this, subID, subDescriptor);
            subscriptionMap.put(subID, subscription);
            BsonObject resp = new BsonObject();
            resp.put(Protocol.RESPONSE_OK, true);
            resp.put(Protocol.SUBRESPONSE_SUBID, subID);
            writeResponse0(Protocol.SUBRESPONSE_FRAME, frame, resp);
            logger.trace("Subscribed channel: {} startSeq {}", channel, startSeq);
        });
    }

    @Override
    public void handleSubClose(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.SUBCLOSE_FRAME, frame, () -> {
            Integer subID = frame.getInteger(Protocol.SUBCLOSE_SUBID);
            if (subID == null) {
                missingField(Protocol.SUBCLOSE_SUBID, Protocol.SUBCLOSE_FRAME);
                return;
            }
            SubscriptionImpl subscription = subscriptionMap.remove(subID);
            if (subscription == null) {
                invalidField(Protocol.SUBCLOSE_SUBID, Protocol.SUBCLOSE_FRAME);
                return;
            }
            subscription.close();
            writeResponseOK(frame);
        });

    }

    @Override
    public void handleUnsubscribe(BsonObject frame) {
        checkContext();


        authoriseThenHandle(Protocol.UNSUBSCRIBE_FRAME, frame, () -> {
            Integer subID = frame.getInteger(Protocol.UNSUBSCRIBE_SUBID);
            if (subID == null) {
                missingField(Protocol.UNSUBSCRIBE_SUBID, Protocol.UNSUBSCRIBE_FRAME);
                return;
            }
            SubscriptionImpl subscription = subscriptionMap.remove(subID);
            if (subscription == null) {
                invalidField(Protocol.UNSUBSCRIBE_SUBID, Protocol.UNSUBSCRIBE_FRAME);
                return;
            }
            subscription.close();
            subscription.unsubscribe();
            writeResponseOK(frame);
        });

    }


    @Override
    public void handleAckEv(BsonObject frame) {
        checkContext();
        authoriseThenHandle(Protocol.ACKEV_FRAME, frame, () -> {
            Integer subID = frame.getInteger(Protocol.ACKEV_SUBID);
            if (subID == null) {
                missingField(Protocol.ACKEV_SUBID, Protocol.ACKEV_FRAME);
                return;
            }
            Integer bytes = frame.getInteger(Protocol.ACKEV_BYTES);
            if (bytes == null) {
                missingField(Protocol.ACKEV_BYTES, Protocol.ACKEV_FRAME);
                return;
            }
            Long pos = frame.getLong(Protocol.ACKEV_POS);
            if (pos == null) {
                missingField(Protocol.ACKEV_POS, Protocol.ACKEV_FRAME);
                return;
            }
            SubscriptionImpl subscription = subscriptionMap.get(subID);
            if (subscription == null) {
                invalidField(Protocol.ACKEV_SUBID, Protocol.ACKEV_FRAME);
                return;
            }
            subscription.handleAckEv(pos, bytes);
        });
    }

    @Override
    public void handleQuery(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.QUERY_FRAME, frame, () -> {
            Integer queryID = frame.getInteger(Protocol.QUERY_QUERYID);
            if (queryID == null) {
                missingField(Protocol.QUERY_QUERYID, Protocol.QUERY_FRAME);
                return;
            }
            String queryName = frame.getString(Protocol.QUERY_NAME);
            if (queryName == null) {
                missingField(Protocol.QUERY_NAME, Protocol.QUERY_FRAME);
                return;
            }
            BsonObject params = frame.getBsonObject(Protocol.QUERY_PARAMS);
            if (params == null) {
                missingField(Protocol.QUERY_PARAMS, Protocol.QUERY_FRAME);
                return;
            }
            QueryImpl query = server.getCqrsManager().getQuery(queryName);
            if (query == null) {
                writeQueryError(Client.ERR_NO_SUCH_QUERY, "No such query " + queryName, queryID);
            } else {
                QueryExecution qe = new ConnectionQueryExecution(this, queryID, query, params);
                queryStates.put(queryID, qe);
                qe.start();
            }
        });

    }


    @Override
    public void handleFindByID(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.FINDBYID_FRAME, frame, () -> {
            String docID = frame.getString(Protocol.FINDBYID_DOCID);
            if (docID == null) {
                missingField(Protocol.FINDBYID_DOCID, Protocol.FINDBYID_FRAME);
                return;
            }
            String binderName = frame.getString(Protocol.FINDBYID_BINDER);
            if (binderName == null) {
                missingField(Protocol.FINDBYID_BINDER, Protocol.FINDBYID_FRAME);
                return;
            }
            Binder binder = server.getBinder(binderName);
            if (binder != null) {
                CompletableFuture<BsonObject> cf = binder.get(docID);
                cf.thenAccept(doc -> {
                    BsonObject resp = new BsonObject();
                    resp.put(Protocol.RESPONSE_OK, true);
                    resp.put(Protocol.FINDRESPONSE_RESULT, doc);
                    writeResponse0(Protocol.RESPONSE_FRAME, frame, resp);
                });
            } else {
                sendErrorResponse(Client.ERR_NO_SUCH_BINDER, "No such binder " + binderName, frame);
            }
        });
    }

    @Override
    public void handleQueryAck(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.QUERYACK_FRAME, frame, () -> {
            Integer queryID = frame.getInteger(Protocol.QUERYACK_QUERYID);
            if (queryID == null) {
                missingField(Protocol.QUERYACK_QUERYID, Protocol.QUERYACK_FRAME);
                return;
            }
            Integer bytes = frame.getInteger(Protocol.QUERYACK_BYTES);
            if (bytes == null) {
                missingField(Protocol.QUERYACK_BYTES, Protocol.QUERYACK_FRAME);
                return;
            }
            QueryExecution queryState = queryStates.get(queryID);
            if (queryState != null) {
                queryState.handleAck(bytes);
            }
        });
    }

    @Override
    public void handlePing(BsonObject frame) {
        checkContext();

        throw new UnsupportedOperationException();
    }

    @Override
    public void handleCommand(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.COMMAND_FRAME, frame, () -> {
            String commandName = frame.getString(Protocol.COMMAND_NAME);
            if (commandName == null) {
                missingField(Protocol.COMMAND_NAME, Protocol.COMMAND_FRAME);
                return;
            }
            BsonObject command = frame.getBsonObject(Protocol.COMMAND_COMMAND);
            if (command == null) {
                missingField(Protocol.COMMAND_COMMAND, Protocol.COMMAND_FRAME);
                return;
            }
            CompletableFuture<Void> cf = server.getCqrsManager().callCommandHandler(commandName, command);
            cf.handle((res, t) -> {
                if (t != null) {
                    sendErrorResponse(Client.ERR_COMMAND_NOT_PROCESSED, t.getMessage(), frame);
                } else {
                    writeResponseOK(frame);
                }
                return null;
            });
        });

    }

    // Admin operations

    @Override
    public void handleListBinders(BsonObject frame) {
        checkContext();
        authoriseThenHandle(Protocol.LIST_BINDERS_FRAME, frame, () -> {
            BsonObject resp = new BsonObject();
            resp.put(Protocol.RESPONSE_OK, true);
            BsonArray arr = new BsonArray(server.listBinders());
            resp.put(Protocol.LISTBINDERS_BINDERS, arr);
            writeResponse0(Protocol.RESPONSE_FRAME, frame, resp);
        });
    }

    @Override
    public void handleCreateBinder(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.CREATEBINDER_NAME, frame, () -> {
            String binderName = frame.getString(Protocol.CREATEBINDER_NAME);
            if (binderName == null) {
                missingField(Protocol.CREATEBINDER_NAME, Protocol.CREATE_BINDER_FRAME);
                return;
            }
            CompletableFuture<Boolean> cf = server.createBinder(binderName);
            cf.handle((res, t) -> {
                if (t != null) {
                    sendErrorResponse(Client.ERR_SERVER_ERROR, "failed to create binder", frame);
                } else {
                    BsonObject resp = new BsonObject();
                    resp.put(Protocol.RESPONSE_OK, true);
                    resp.put(Protocol.CREATEBINDER_RESPONSE_EXISTS, !res);
                    writeResponse0(Protocol.RESPONSE_FRAME, frame, resp);
                }
                return null;
            });
        });

    }

    @Override
    public void handleListChannels(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.LIST_CHANNELS_FRAME, frame, () -> {
            BsonObject resp = new BsonObject();
            resp.put(Protocol.RESPONSE_OK, true);
            BsonArray arr = new BsonArray(server.listChannels());
            resp.put(Protocol.LISTCHANNELS_CHANNELS, arr);
            writeResponse0(Protocol.RESPONSE_FRAME, frame, resp);
        });
    }

    @Override
    public void handleCreateChannel(BsonObject frame) {
        checkContext();

        authoriseThenHandle(Protocol.CREATECHANNEL_NAME, frame, () -> {
            String channelName = frame.getString(Protocol.CREATECHANNEL_NAME);
            if (channelName == null) {
                missingField(Protocol.CREATECHANNEL_NAME, Protocol.CREATE_CHANNEL_FRAME);
                return;
            }
            CompletableFuture<Boolean> cf = server.createChannel(channelName);
            cf.handle((res, t) -> {
                if (t != null) {
                    sendErrorResponse(Client.ERR_SERVER_ERROR, "failed to create channel", frame);
                } else {
                    BsonObject resp = new BsonObject();
                    resp.put(Protocol.RESPONSE_OK, true);
                    resp.put(Protocol.CREATECHANNEL_RESPONSE_EXISTS, !res);
                    writeResponse0(Protocol.RESPONSE_FRAME, frame, resp);
                }
                return null;
            });
        });
    }


    private void writeResponse0(String responseFrameType, BsonObject requestFrame, BsonObject responseFrame) {
        Integer requestID = requestFrame.getInteger(Protocol.REQUEST_REQUEST_ID);
        if (requestID != null) {
            responseFrame.put(Protocol.RESPONSE_REQUEST_ID, requestID);
            writeResponse(responseFrameType, responseFrame);
        } else {
            logAndClose("No request id in frame");
        }
    }

    private void writeResponseOK(BsonObject frame) {
        BsonObject resp = new BsonObject();
        resp.put(Protocol.RESPONSE_OK, true);
        writeResponse0(Protocol.RESPONSE_FRAME, frame, resp);
    }

    private void authoriseThenHandle(String frameType, BsonObject frame, Runnable action) {
        CompletableFuture<Boolean> authorisedCF = user.isAuthorised(frameType);
        authorisedCF.whenComplete((res, ex) -> {
            if (ex != null) {
                String msg = "Authorisation failed";
                logger.error(msg, ex);
                sendErrorResponse(Client.ERR_SERVER_ERROR, msg, frame);
                close();
            } else {
                if (!res) {
                    sendErrorResponse(Client.ERR_NOT_AUTHORISED, "User is not authorised", frame);
                    close();
                } else {
                    // OK
                    action.run();
                }
            }
        });
    }

    protected Buffer writeQueryResult(BsonObject doc, int queryID, boolean last) {
        BsonObject res = new BsonObject();
        res.put(Protocol.QUERYRESULT_OK, true);
        res.put(Protocol.QUERYRESULT_QUERYID, queryID);
        res.put(Protocol.QUERYRESULT_RESULT, doc);
        res.put(Protocol.QUERYRESULT_LAST, last);
        return writeResponse(Protocol.QUERYRESULT_FRAME, res);
    }

    private Buffer writeQueryError(int errCode, String errMsg, int queryID) {
        BsonObject res = new BsonObject();
        res.put(Protocol.QUERYRESULT_OK, false);
        res.put(Protocol.QUERYRESULT_QUERYID, queryID);
        res.put(Protocol.QUERYRESULT_LAST, true);
        res.put(Protocol.RESPONSE_ERRCODE, errCode);
        res.put(Protocol.RESPONSE_ERRMSG, errMsg);
        return writeResponse(Protocol.QUERYRESULT_FRAME, res);
    }

    protected Buffer writeResponse(String frameName, BsonObject frame) {

        Buffer buff = Protocol.encodeFrame(frameName, frame);
        // TODO compare performance of writing directly in all cases and via context
        Context curr = Vertx.currentContext();
        if (curr != context) {
            context.runOnContext(v -> transportConnection.write(buff));
        } else {
            transportConnection.write(buff);
        }
        return buff;
    }

    private void checkWrap(int i) {
        // Sanity check - wrap around - won't happen but better to close connection than give incorrect behaviour
        if (i == Integer.MIN_VALUE) {
            String msg = "int wrapped!";
            logger.error(msg);
            close();
        }
    }

    private void missingField(String fieldName, String frameType) {
        logger.warn("protocol error: missing {} in {}. connection will be closed", fieldName, frameType);
        close();
    }

    private void invalidField(String fieldName, String frameType) {
        logger.warn("protocol error: invalid {} in {}. connection will be closed", fieldName, frameType);
        close();
    }

    private void logAndClose(String exceptionMessage) {
        logger.error("{}, Connection will be closed", exceptionMessage);
        close();
    }

    private void sendConnectErrorResponse(int errCode, String errMsg) {
        BsonObject resp = new BsonObject();
        resp.put(Protocol.RESPONSE_OK, false);
        resp.put(Protocol.RESPONSE_ERRCODE, errCode);
        resp.put(Protocol.RESPONSE_ERRMSG, errMsg);
        writeResponse(Protocol.RESPONSE_FRAME, resp);
    }

    private void sendErrorResponse(int errCode, String errMsg, BsonObject frame) {
        BsonObject resp = new BsonObject();
        Integer requestID = frame.getInteger(Protocol.REQUEST_REQUEST_ID);
        if (requestID != null) {
            resp.put(Protocol.RESPONSE_REQUEST_ID, requestID);
            resp.put(Protocol.RESPONSE_OK, false);
            resp.put(Protocol.RESPONSE_ERRCODE, errCode);
            resp.put(Protocol.RESPONSE_ERRMSG, errMsg);
            writeResponse(Protocol.RESPONSE_FRAME, resp);
        } else {
            logAndClose(errMsg + ": " + errCode);
        }
    }

    // Sanity check - this should always be executed using the correct context
    private void checkContext() {
        if (Vertx.currentContext() != context) {
            logger.trace("Wrong context!! " + Thread.currentThread() + " expected " + context, new Exception());
            throw new IllegalStateException("Wrong context!");
        }
    }

    protected void removeQueryState(int queryID) {
        checkContext();
        queryStates.remove(queryID);
    }

    private void close() {
        checkContext();
        if (closed) {
            return;
        }

        user = new UnauthorizedUser();

        for (QueryExecution queryState : queryStates.values()) {
            queryState.close();
        }
        queryStates.clear();
        closed = true;
        transportConnection.close();
    }

    protected ServerImpl server() {
        return server;
    }

}

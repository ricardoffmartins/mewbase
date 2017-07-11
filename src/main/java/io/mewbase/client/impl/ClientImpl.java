package io.mewbase.client.impl;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.mewbase.client.*;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.impl.Protocol;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 22/09/16.
 */
public class ClientImpl implements Client, ClientFrameHandler {

    private final static Logger logger = LoggerFactory.getLogger(ClientImpl.class);

    private final AtomicInteger sessionSeq = new AtomicInteger();
    private final AtomicInteger requestIDSequence = new AtomicInteger();
    private final Map<Integer, ProducerImpl> producerMap = new ConcurrentHashMap<>();
    private final Map<Integer, SubscriptionImpl> subscriptionMap = new ConcurrentHashMap<>();
    private final Map<Integer, Consumer<BsonObject>> responseHandlers = new ConcurrentHashMap<>();
    private final Map<Integer, Consumer<QueryResult>> queryResultHandlers = new ConcurrentHashMap<>();
    private final Vertx vertx;
    private final NetClient netClient;
    private final ClientOptions clientOptions;
    private final boolean ownVertx;

    private NetSocket netSocket;
    private boolean connecting;
    private Queue<Buffer> bufferedWrites = new ConcurrentLinkedQueue<>();
    private Consumer<BsonObject> connectResponse;
    private long pingTimerID = -1;
    private boolean closed;

    ClientImpl(ClientOptions clientOptions) {
        this(Vertx.vertx(), clientOptions, true);
    }

    ClientImpl(Vertx vertx, ClientOptions clientOptions) {
        this(vertx, clientOptions, false);
    }

    ClientImpl(Vertx vertx, ClientOptions clientOptions, boolean ownVertx) {
        this.vertx = vertx;
        this.netClient = vertx.createNetClient(clientOptions.getNetClientOptions());
        this.clientOptions = clientOptions;
        this.ownVertx = ownVertx;
    }

    @Override
    public Producer createProducer(String channel) {
        int id = sessionSeq.getAndIncrement();
        ProducerImpl prod = new ProducerImpl(this, channel, id);
        producerMap.put(id, prod);
        return prod;
    }

    @Override
    public CompletableFuture<Subscription> subscribe(SubDescriptor descriptor, Consumer<ClientDelivery> handler) {
        CompletableFuture<Subscription> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();

        if (descriptor.getChannel() == null) {
            throw new IllegalArgumentException("No channel in SubDescriptor");
        }
        frame.put(Protocol.SUBSCRIBE_CHANNEL, descriptor.getChannel());

        // @see wire_protocol.md
        if (descriptor.getStartEventNum() != SubDescriptor.DEFAULT_START_NUM &&
            descriptor.getStartTimestamp() != SubDescriptor.DEFAULT_START_TIME) {
            throw new IllegalArgumentException("Cannot set both start position and start timestamp");
        }
        frame.put(Protocol.SUBSCRIBE_STARTPOS, descriptor.getStartEventNum());
        frame.put(Protocol.SUBSCRIBE_STARTTIMESTAMP, descriptor.getStartTimestamp());

        frame.put(Protocol.SUBSCRIBE_DURABLEID, descriptor.getDurableID());
        frame.put(Protocol.SUBSCRIBE_FILTER_NAME, descriptor.getFilterName());
        write(cf, Protocol.SUBSCRIBE_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            if (ok) {
                int subID = resp.getInteger(Protocol.SUBRESPONSE_SUBID);
                SubscriptionImpl sub = new SubscriptionImpl(subID, descriptor.getChannel(), this, handler);
                subscriptionMap.put(subID, sub);
                cf.complete(sub);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }


    @Override
    public CompletableFuture<Void> publish(String channel, BsonObject event) {
        return doPublish(channel, -1, event);
    }

    @Override
    public CompletableFuture<Void> publish(String channel, BsonObject event, Function<BsonObject, String> partitionFunc) {
        // TODO partitions
        return doPublish(channel, -1, event);
    }

    @Override
    public CompletableFuture<BsonObject> findByID(String binderName, String id) {
        CompletableFuture<BsonObject> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        frame.put(Protocol.FINDBYID_BINDER, binderName);
        frame.put(Protocol.FINDBYID_DOCID, id);
        write(cf, Protocol.FINDBYID_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            if (ok) {
                BsonObject result = resp.getBsonObject(Protocol.FINDRESPONSE_RESULT);
                cf.complete(result);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }

    private void writeQuery(BsonObject frame, Consumer<QueryResult> resultHandler, CompletableFuture cf) {
        int queryID = requestIDSequence.getAndIncrement();
        frame.put(Protocol.QUERY_QUERYID, queryID);
        queryResultHandlers.put(queryID, resultHandler);
        write(cf, Protocol.QUERY_FRAME, frame);
    }


    @Override
    public void executeQuery(String queryName,
                             Consumer<QueryResult> resultHandler,
                             Consumer<Throwable> exceptionHandler) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        if (exceptionHandler != null) {
            cf.exceptionally(t -> {
                exceptionHandler.accept(t);
                return null;
            });
        }
        BsonObject frame = new BsonObject();
        frame.put(Protocol.QUERY_NAME, queryName);
        BsonObject emptyParams = new BsonObject();
        frame.put(Protocol.QUERY_PARAMS, emptyParams);
        int queryID = requestIDSequence.getAndIncrement();
        frame.put(Protocol.QUERY_QUERYID, queryID);
        queryResultHandlers.put(queryID, resultHandler);
        write(cf, Protocol.QUERY_FRAME, frame);
    }

    // Admin operations

    @Override
    public CompletableFuture<BsonArray> listBinders() {
        CompletableFuture<BsonArray> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        write(cf, Protocol.LIST_BINDERS_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            if (ok) {
                BsonArray binders = resp.getBsonArray(Protocol.LISTBINDERS_BINDERS);
                cf.complete(binders);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Boolean> createBinder(String binderName) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        frame.put(Protocol.CREATEBINDER_NAME, binderName);
        write(cf, Protocol.CREATE_BINDER_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            boolean exists = resp.getBoolean(Protocol.CREATEBINDER_RESPONSE_EXISTS);
            if (ok) {
                cf.complete(!exists);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<BsonArray> listChannels() {
        CompletableFuture<BsonArray> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        write(cf, Protocol.LIST_CHANNELS_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            if (ok) {
                BsonArray channels = resp.getBsonArray(Protocol.LISTCHANNELS_CHANNELS);
                cf.complete(channels);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Boolean> createChannel(String channelName) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        frame.put(Protocol.CREATECHANNEL_NAME, channelName);
        write(cf, Protocol.CREATE_CHANNEL_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            boolean exists = resp.getBoolean(Protocol.CREATECHANNEL_RESPONSE_EXISTS);
            if (ok) {
                cf.complete(!exists);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Void> sendCommand(String commandName, BsonObject command) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        frame.put(Protocol.COMMAND_NAME, commandName);
        frame.put(Protocol.COMMAND_COMMAND, command);
        write(cf, Protocol.COMMAND_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            if (ok) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        if (pingTimerID != -1) {
            vertx.cancelTimer(pingTimerID);
        }
        netClient.close();
        if (ownVertx) {
            AsyncResCF<Void> cf = new AsyncResCF<>();
            vertx.close(cf);
            return cf;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    // FrameHandler

    @Override
    public void handleQueryResult(int size, BsonObject resp) {
        Integer rQueryID = resp.getInteger(Protocol.QUERYRESULT_QUERYID);
        Consumer<QueryResult> qrh = queryResultHandlers.get(rQueryID);
        if (qrh == null) {
            throw new IllegalStateException("Can't find query result handler");
        }
        // FIXME - what to do with this?
        boolean ok = resp.getBoolean(Protocol.QUERYRESULT_OK);
        boolean last = resp.getBoolean(Protocol.QUERYRESULT_LAST);
        QueryResult qr = new QueryResultImpl(resp.getBsonObject(Protocol.QUERYRESULT_RESULT), size, last, rQueryID);
        try {
            qrh.accept(qr);
        } finally {
            if (last) {
                queryResultHandlers.remove(rQueryID);
            }
        }
    }

    @Override
    public void handleRecev(int size, BsonObject frame) {
        int subID = frame.getInteger(Protocol.RECEV_SUBID);
        SubscriptionImpl sub = subscriptionMap.get(subID);
        if (sub == null) {
            // No subscription for this - maybe closed - ignore
        } else {
            sub.handleRecevFrame(size, frame);
        }
    }

    @Override
    public void handleSubResponse(BsonObject frame) {
        handleResponse(frame);
    }

    @Override
    public void handleResponse(BsonObject frame) {
        if (connecting) {
            connectResponse.accept(frame);
        } else {
            Integer requestID = frame.getInteger(Protocol.RESPONSE_REQUEST_ID);
            if (requestID == null) {
                throw new IllegalStateException("No request id in response: " + frame);
            }
            Consumer<BsonObject> respHandler = responseHandlers.remove(requestID);
            if (respHandler == null) {
                throw new IllegalStateException("Unexpected response");
            }
            respHandler.accept(frame);
        }
    }

    protected synchronized void write(CompletableFuture cf, String frameType, BsonObject frame) {
        write(cf, frameType, frame, fr -> {
        });
    }

    protected synchronized void write(CompletableFuture cf, String frameType, BsonObject frame,
                                      Consumer<BsonObject> respHandler) {
        if (closed) {
            throw new MewException("Connection is closed");
        }
        if (respHandler != null) {
            Integer requestID = requestIDSequence.getAndIncrement();
            frame.put(Protocol.RESPONSE_REQUEST_ID, requestID);
            responseHandlers.put(requestID, respHandler);
        }
        Buffer buff = Protocol.encodeFrame(frameType, frame);
        if (connecting || netSocket == null) {
            if (!connecting) {
                connect(cf);
            }
            bufferedWrites.add(buff);
        } else {
            write(buff);
        }
    }

    protected void write(Buffer buff) {
        if (netSocket == null) {
            throw new MewException("Not connected");
        }
        netSocket.write(buff);
    }

    private void connect(CompletableFuture cfConnect) {
        AsyncResCF<NetSocket> cf = new AsyncResCF<>();
        netClient.connect(clientOptions.getPort(), clientOptions.getHost(), cf);
        connecting = true;

        cf.thenAccept(ns -> sendConnect(cfConnect, ns)).exceptionally(t -> {
            cfConnect.completeExceptionally(t);
            return null;
        });
    }

    protected void doUnsubscribe(int subID) {
        subscriptionMap.remove(subID);
        BsonObject frame = new BsonObject();
        frame.put(Protocol.UNSUBSCRIBE_SUBID, subID);
        CompletableFuture cf = new CompletableFuture();
        write(cf, Protocol.UNSUBSCRIBE_FRAME, frame);
    }

    protected void doSubClose(int subID) {
        subscriptionMap.remove(subID);
        BsonObject frame = new BsonObject();
        frame.put(Protocol.SUBCLOSE_SUBID, subID);
        CompletableFuture cf = new CompletableFuture();
        write(cf, Protocol.SUBCLOSE_FRAME, frame);
    }

    protected void doAckEv(int subID, long pos, int sizeBytes) {
        BsonObject frame = new BsonObject();
        frame.put(Protocol.ACKEV_SUBID, subID);
        frame.put(Protocol.ACKEV_POS, pos);
        frame.put(Protocol.ACKEV_BYTES, sizeBytes);
        Buffer buffer = Protocol.encodeFrame(Protocol.ACKEV_FRAME, frame);
        write(buffer);
    }

    protected void doQueryAck(int queryID, int bytes) {
        BsonObject frame = new BsonObject();
        frame.put(Protocol.QUERYACK_QUERYID, queryID);
        frame.put(Protocol.QUERYACK_BYTES, bytes);
        Buffer buffer = Protocol.encodeFrame(Protocol.QUERYACK_FRAME, frame);
        write(buffer);
    }

    protected void writePing() {
        BsonObject frame = new BsonObject();
        Buffer buffer = Protocol.encodeFrame(Protocol.PING_FRAME, frame);
        write(buffer);
    }

    protected CompletableFuture<Void> doPublish(String channel, int producerID, BsonObject event) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        frame.put(Protocol.PUBLISH_CHANNEL, channel);
        frame.put(Protocol.PUBLISH_SESSID, producerID);
        frame.put(Protocol.PUBLISH_EVENT, event);
        write(cf, Protocol.PUBLISH_FRAME, frame, resp -> {
            boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
            if (ok) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(responseToException(resp));
            }
        });
        return cf;
    }

    protected void removeProducer(int producerID) {
        producerMap.remove(producerID);
    }

    private MewException responseToException(BsonObject resp) {
        return new MewException(resp.getString(Protocol.RESPONSE_ERRMSG),
                resp.getInteger(Protocol.RESPONSE_ERRCODE));
    }

    private synchronized void sendConnect(CompletableFuture cfConnect, NetSocket ns) {
        netSocket = ns;
        netSocket.handler(new Protocol(this).recordParser());
        netSocket.closeHandler(this::closeHandler);

        BsonObject authInfo = clientOptions.getAuthInfo();

        // Send the CONNECT frame
        BsonObject frame = new BsonObject();
        frame.put(Protocol.CONNECT_VERSION, "0.1");
        frame.put(Protocol.CONNECT_AUTH_INFO, authInfo);

        Buffer buffer = Protocol.encodeFrame(Protocol.CONNECT_FRAME, frame);
        connectResponse = resp -> connected(cfConnect, resp);
        netSocket.write(buffer);
    }

    private synchronized void closeHandler(Void v) {
        logger.trace("Connection has been closed");
        closed = true;
        netSocket = null;
    }

    private synchronized void connected(CompletableFuture cfConnect, BsonObject resp) {
        connecting = false;
        boolean ok = resp.getBoolean(Protocol.RESPONSE_OK);
        if (ok) {
            while (true) {
                Buffer buff = bufferedWrites.poll();
                if (buff == null) {
                    break;
                }
                netSocket.write(buff);
            }
            startPinger();
        } else {
            cfConnect.completeExceptionally(new MewException(resp.getString(Protocol.RESPONSE_ERRMSG),
                    resp.getInteger(Protocol.RESPONSE_ERRCODE)));
        }
    }

    private void startPinger() {
        pingTimerID = vertx.setPeriodic(clientOptions.getPingPeriod(), id -> writePing());
    }

    private class QueryResultImpl implements QueryResult {

        private final BsonObject document;
        private final int bytes;
        private final boolean last;
        private final int queryID;

        public QueryResultImpl(BsonObject document, int bytes, boolean last, int queryID) {
            this.document = document;
            this.bytes = bytes;
            this.last = last;
            this.queryID = queryID;
        }

        @Override
        public BsonObject document() {
            return document;
        }

        @Override
        public void acknowledge() {
            doQueryAck(queryID, bytes);
        }

        @Override
        public boolean isLast() {
            return last;
        }

    }

}

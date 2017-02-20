package io.mewbase.server.impl.process;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.Client;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.ProcessStageDefinition;
import io.mewbase.server.StageContext;
import io.mewbase.server.impl.ServerImpl;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by tim on 17/01/17.
 */
public class SubscriberProcessStage implements ProcessStage {

    private final static Logger logger = LoggerFactory.getLogger(SubscriberProcessStage.class);

    private final Process process;
    private final ProcessStageDefinition stageDefinition;
    private final Map<String, ProcessInstance> instances = new HashMap<>();
    private final boolean initial;
    private final ServerImpl server;
    private Client client;

    SubscriberProcessStage(Process process, ProcessStageDefinition stageDefinition, boolean initial,
                           ServerImpl server) {
        this.process = process;
        this.stageDefinition = stageDefinition;
        this.initial = initial;
        this.server = server;
    }

    @Override
    public String getName() {
        return stageDefinition.getName();
    }

    public void init() {
        // TODO share clients for same options
        client = Client.newClient(stageDefinition.getClientOptions());
        client.subscribe(new SubDescriptor().setChannel(stageDefinition.getFromChannel()), del -> {
            handleEvent(del.event());
        });
        System.out.println("Created subscription to channel " + stageDefinition.getFromChannel() + " on server at port " + stageDefinition.getClientOptions().getPort());
    }

    public void close() {
        client.close();
    }

    public void stageStarted(String id, ProcessInstance instance) {
        instances.put(id, instance);
        logger.trace("Added instance id " + id + " to stage " + this);
    }

    private void handleEvent(BsonObject event) {
        logger.trace("Process stage " + stageDefinition + " received event: " + event);
        ProcessInstance instance = null;
        try {
            if (stageDefinition.getFilterFunction().apply(event)) {
                String id = stageDefinition.getIdentifyFunction().apply(event);
                instance = instances.get(id);

                if (instance == null) {
                    logger.trace("Couldn't find instance with id " + id);
                }
                if (instance == null && initial) {

                    // No current process instance - create one
                    instance = new ProcessInstance(id, process, server);
                    instances.put(id, instance);
                    logger.trace("created new instance with id " + id);
                } else {
                    logger.trace("Found instance " + instance);
                }
                if (instance != null) {
                    stageDefinition.getEventHandler().accept(event, new SimpleStageContext(instance));
                }
            }
        } catch (Throwable t) {
            callExceptionHandler(t, instance);
        }
    }

    private void callExceptionHandler(Throwable t, ProcessInstance instance) {
        BiConsumer<BsonObject, Throwable> exceptionHandler = process.getProcessDefinition().getExceptionHandler();
        if (exceptionHandler != null) {
            try {
                exceptionHandler.accept(instance.getState(), t);
            } catch (Throwable t2) {
                logger.warn("Unhandled Throwable in exception handler", t2);
            }
        } else {
            logger.debug("Process stage timed out");
        }
    }

    private class SimpleStageContext implements StageContext {

        private final ProcessInstance instance;
        private boolean inThenDo;

        public SimpleStageContext(ProcessInstance instance) {
            this.instance = instance;
        }

        @Override
        public BsonObject getState() {
            return instance.getState();
        }

        @Override
        public void complete() {
            instances.remove(instance.getID());
            Consumer<StageContext> action = stageDefinition.getAction();
            if (action != null && !inThenDo) {
                inThenDo = true;
                try {
                    action.accept(this);
                } catch (Throwable t) {
                    callExceptionHandler(t, instance);
                }
            } else {
                instance.stageComplete();
            }
        }

        @Override
        public void fail(Exception e) {
            instance.stageFailed(e);
        }
    }

}

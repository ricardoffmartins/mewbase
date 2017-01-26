package io.mewbase.server.impl.process;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.impl.ServerImpl;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 17/01/17.
 */
public class ProcessInstance {

    private final static Logger logger = LoggerFactory.getLogger(ProcessInstance.class);

    private final String id;
    private final BsonObject state;
    private final Process process;
    private final ServerImpl server;
    private int stageNumber;
    private ProcessStage currentStage;

    ProcessInstance(String id, Process process, ServerImpl server) {
        this.id = id;
        this.process = process;
        this.state = new BsonObject();
        this.server = server;
        setStage();
    }

    public BsonObject getState() {
        return state;
    }

    public void stageComplete() {
        stageNumber++;
        CompletableFuture<Void> cfSave = saveState();
        cfSave.whenComplete((v, t) -> {
            if (t == null) {
                if (stageNumber == process.getNumStages()) {
                    // Now complete
                    // TODO
                    logger.trace("Process instance complete!");
                } else {
                    setStage();
                }
            } else {
                handleException(t);
            }
        });

    }

    public void stageFailed(Exception e) {
        // TODO??
    }

    public String getID() {
        return id;
    }

    private void handleException(Throwable t) {
        // TODO
    }

    private void setStage() {
        currentStage = process.getStage(stageNumber);
        currentStage.stageStarted(id, this);
    }

    private CompletableFuture<Void> saveState() {
        return server.getProcessStatesBinder().put(id, state);
    }




}

package io.mewbase.server.impl.process;

import io.mewbase.server.ProcessDefinition;
import io.mewbase.server.ProcessStageDefinition;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tim on 17/01/17.
 */
public class Process {

    private final List<ProcessStage> processStages = new ArrayList<>();
    private final ProcessDefinition processDefinition;

    public Process(ProcessDefinition processDefinition, Vertx vertx) {
        this.processDefinition = processDefinition;
        boolean initial = true;
        for (ProcessStageDefinition processStageDefinition: processDefinition.getStageDefinitions()) {
            ProcessStage stage = new SubscriberProcessStage(this, processStageDefinition, initial, vertx);
            processStages.add(stage);
            initial = false;
        }
    }

    public int getNumStages() {
        return processDefinition.getStageDefinitions().size();
    }

    public ProcessStage getStage(int pos) {
        return processStages.get(pos);
    }

    public void start() {
        for (ProcessStage processStage : processStages) {
            processStage.init();
        }
    }

    public void stop() {
        for (ProcessStage stage: processStages) {
            stage.close();
        }
    }

    public String getName() {
        return processDefinition.getName();
    }

    public ProcessDefinition getProcessDefinition() {
        return processDefinition;
    }

}

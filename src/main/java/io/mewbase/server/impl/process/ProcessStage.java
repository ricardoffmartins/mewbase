package io.mewbase.server.impl.process;


/**
 * Created by tim on 24/01/17.
 */
public interface ProcessStage {

    String getName();

    void init();

    void close();

    void stageStarted(String id, ProcessInstance instance);

}

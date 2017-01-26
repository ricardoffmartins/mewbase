package io.mewbase.server.impl.process;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tim on 17/01/17.
 */
public class ProcessManager {

    private final Map<String, Process> processMap = new HashMap<>();

    public void registerProcess(Process process) {
        if (processMap.containsKey(process.getName())) {
            throw new IllegalArgumentException("Already a process with name " + process.getName());
        }
        processMap.put(process.getName(), process);
    }

    public void start() throws Exception {

        // TODO load from persistent storage

//        for (Process process: processMap.values()) {
//            process.start();
//        }

    }


}

package org.pcg.walrus.server.execution.resource;

import scala.annotation.meta.param;

import java.util.HashMap;
import java.util.Map;

/**
 * parse from job_resource
 */
public class CustomResource implements WResource {

    private Map<String, String> resource;

    public CustomResource(String resourceStr) {
        resource = parseParam(resourceStr, "--");
    }

    @Override
    public int executorNum() {
        int executors = 2;
        String coreNum = resource.getOrDefault("num-executors", "2");
        try {
            executors = Integer.parseInt(coreNum);
        } catch (NumberFormatException e) {}
        return executors;
    }

    @Override
    public int coreNum() {
        int cores = 1;
        String coreNum = resource.getOrDefault("executor-cores", "1");
        try {
            cores = Integer.parseInt(coreNum);
        } catch (NumberFormatException e) {}
        return cores;
    }

    @Override
    public String driverMem() {
        return resource.getOrDefault("driver-memory", "2g");
    }

    @Override
    public String executorMem() {
        return resource.getOrDefault("executor-memory", "2g");
    }

    /*
     * --num-executors 10 --driver-memory 10g --executor-memory 4g  --executor-cores 1
     * {"num-executors":10, "driver-memory": "10g"}
     */
    private Map<String, String> parseParam(String str, String delem){
        Map<String, String> param = new HashMap<>();
        String[] args = str.split(" ");
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith(delem)) {
                param.put(args[i].replace(delem, ""), args[++i]);
            }
        }
        return param;
    }

    @Override
    public String toString() {
        return resource.toString();
    }
}

package org.pcg.walrus.server.execution.resource;

public interface WResource {

    /**
     * executor number
     */
    public int executorNum();

    /**
     * core number
     */
    public int coreNum();

    /**
     * spark driver memory
     */
    public String driverMem();

    /**
     * spark executor memory
     */
    public String executorMem();
}

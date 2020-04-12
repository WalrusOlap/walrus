package org.pcg.walrus.core.scheduler;

/**
 * job monitor
 */
public interface TaskMonitor {

    public void run(long jobId);

    public void kill(long jobId);
}

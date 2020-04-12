package org.pcg.walrus.server.execution.submit;

import org.pcg.walrus.server.execution.resource.WResource;
import org.pcg.walrus.server.model.Task;

/**
 * submit spark task
 */
public interface TaskSubmitor {

    public void submitTask(Task task, WResource resource) throws Exception;

}

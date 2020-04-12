package org.pcg.walrus.server.execution.submit;

import org.pcg.walrus.server.execution.resource.WResource;
import org.pcg.walrus.server.model.Task;
import org.springframework.stereotype.Component;

@Component
public class ThriftSubmitor implements TaskSubmitor {

    @Override
    public void submitTask(Task task, WResource resource) throws Exception {

    }
}

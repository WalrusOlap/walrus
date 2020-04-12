package org.pcg.walrus.server.execution.submit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class SubmitorFactory implements ApplicationRunner {

    private static final String SUBMIT_TYPE_LOCAL = "local";
    private static final String SUBMIT_TYPE_THRIFT = "thrift";

    @Autowired
    private LocalSubmitor localSubmitor;
    @Autowired
    private ThriftSubmitor thriftSubmitor;

    private Map<String, TaskSubmitor> submitorMap;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        submitorMap = new HashMap<>();
        submitorMap.put(SUBMIT_TYPE_LOCAL, localSubmitor);
        submitorMap.put(SUBMIT_TYPE_THRIFT, thriftSubmitor);
    }

    /**
     * get submitor by mode
     */
    public TaskSubmitor getSubmitor(String mode) {
        return submitorMap.get(mode);
    }
}


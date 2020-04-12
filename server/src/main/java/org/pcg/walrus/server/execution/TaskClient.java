package org.pcg.walrus.server.execution;

import org.apache.commons.lang.StringUtils;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.core.parse.ParserAgent;
import org.pcg.walrus.core.plan.LogicalPlan;
import org.pcg.walrus.core.scheduler.rpc.RpcClient;
import org.pcg.walrus.core.scheduler.rpc.RpcMessage;
import org.pcg.walrus.core.scheduler.rpc.RpcServiceManager;
import org.pcg.walrus.core.scheduler.rpc.RpcServiceManager.SharkRunner;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.server.dao.TaskDao;
import org.pcg.walrus.server.execution.resource.CustomResource;
import org.pcg.walrus.server.execution.resource.PlanResource;
import org.pcg.walrus.server.execution.resource.WResource;
import org.pcg.walrus.server.execution.submit.SubmitorFactory;
import org.pcg.walrus.server.execution.submit.TaskSubmitor;
import org.pcg.walrus.server.model.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;

@Component
public class TaskClient {

    private static final Logger log = LoggerFactory.getLogger(TaskClient.class);

    @Autowired
    private TaskDao taskDao;
    @Autowired
    private MetaClient metaClient;
    @Autowired
    private SubmitorFactory submitorFactory;

    private @Value("${spark.resource.memoryUnit}") int memoryUnit;
    private @Value("${spark.resource.executorUnit}") int executorUnit;
    private @Value("${spark.resource.maxExecutor}") int maxExecutor;
    private @Value("${spark.resource.minExecutor}") int minExecutor;

    /**
     * submit task
     * @param task
     * @throws WServerException
     */
    public void submit(Task task) throws WServerException, WCoreException {
        String level = judgeLevel(task);
        LogicalPlan plan = ParserAgent.parseJob(metaClient.getClient(), level, task.getTaskId(), task.getFormat(), task.getLogic());
        // update job info
        taskDao.updateQuery(task.getTaskId(), plan.getTable(), TimeUtil.DateToString(plan.getBday(), "yyyy-MM-dd HH:mm:ss"), TimeUtil.DateToString(plan.getEday(), "yyyy-MM-dd HH:mm:ss"));
        try {
            switch (level) {
                case MetaConstants.CLUSTER_LEVEL_OFFLINE:
                    submitOfflineTask(task, plan);
                    break;
                case MetaConstants.CLUSTER_LEVEL_ONLINE:
                    submitOnlineTask(task, plan);
                    break;
                default:
                    throw new Exception("unknown job level: " + level);
            }
        } catch (Exception e) {
            throw new WServerException(e.getMessage());
        }
    }

    /**
     * kill task
     */
    public void kill(long taskId) throws Exception {
        SharkRunner runner = RpcServiceManager.getTaskRunner(metaClient.getZkClient(), taskId);
        RpcMessage msg = new RpcMessage(RpcMessage.METHOD_KILL, taskId);
        RpcClient.sendMessage(runner.address, runner.port, msg);
    }

    // submit offline task
    private void submitOfflineTask(Task task, LogicalPlan plan) throws Exception {
        TaskSubmitor submitor = submitorFactory.getSubmitor(task.getSubmitType());
        if(submitor == null) throw new WServerException("unknown submit type: " + task.getSubmitType());
        // get task resource
        WResource resource = null;
        if(StringUtils.isBlank(task.getResource()))
            resource = new PlanResource(plan, memoryUnit, executorUnit, maxExecutor, minExecutor);
        else resource = new CustomResource(task.getResource());
        // update resource
        taskDao.updateResource(task.getTaskId(), resource.toString());
        // submit task
        log.info(String.format("submit offline task: %d", task.getTaskId()));
        submitor.submitTask(task, resource);
    }

    // submit online task
    private void submitOnlineTask(Task task, LogicalPlan plan) throws Exception {
        List<SharkRunner> runners = RpcServiceManager.availableRunners(metaClient.getZkClient(), MetaConstants.CLUSTER_LEVEL_ONLINE);
        // if no online server avaliable, submit offline task
        if(runners.size() > 0) {
            // TODO choose online server by load balance
            // get a random runner
            SharkRunner runner = runners.get(new Random().nextInt(runners.size()));
            RpcMessage msg = new RpcMessage(RpcMessage.METHOD_RUN, task.getTaskId());
            log.info(String.format("submit online task: %d", task.getTaskId()));
            RpcClient.sendMessage(runner.address, runner.port, msg);
        } else submitOfflineTask(task, plan);
    }

    /**
     * judge task level
     * @throws WServerException
     */
    private String judgeLevel(Task task) throws WServerException {
        String[] levels = new String[] {MetaConstants.CLUSTER_LEVEL_ONLINE, MetaConstants.CLUSTER_LEVEL_OFFLINE};
        String errorMsg = null;
        // try every level and return level with high priority
        for(String level: levels) {
            try {
                ParserAgent.parseJob(metaClient.getClient(), level, task.getTaskId(), task.getFormat(), task.getLogic());
                return level;
            } catch (WCoreException e) {
                errorMsg = e.getErrorMsg();
            }
        }
        throw new WServerException(errorMsg);
    }
}

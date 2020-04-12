package org.pcg.walrus.server.execution.submit;

import com.google.common.base.Joiner;
import org.apache.commons.exec.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.pcg.walrus.common.exception.WException;
import org.pcg.walrus.common.util.EncryptionUtil;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.server.dao.TaskDao;
import org.pcg.walrus.server.execution.resource.WResource;
import org.pcg.walrus.server.model.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.io.File;

/**
 * submit by local spark-submit task
 */
@Component
public class LocalSubmitor implements TaskSubmitor {

    private static final Logger log = LoggerFactory.getLogger(LocalSubmitor.class);

    @Autowired
    private TaskDao taskDao;
    private @Value("${spark.script.path}") String scriptPath;
    private @Value("${spark.submit}") String header;
    private @Value("${spark.runtime.class}") String RUNNER_CLASS;
    private @Value("${spark.runtime.jar}") String RUNNER_JAR;

    private @Value("${zk.url}") String zkUrl;
    private @Value("${zk.timeout}") int zkTimeout;
    private @Value("${spring.datasource.url}") String dbUrl;
    private @Value("${spring.datasource.username}") String dbUser;
    private @Value("${spring.datasource.password}") String dbPwd;
    private @Value("${spring.datasource.driver-class-name}") String dbDriver;

    // submit local job
    public void submitTask(Task task, WResource resource) throws Exception {
        List<String> cmds = new ArrayList<String>();
        String exeFile = String.format("%s/%s/%d_%d.sh", scriptPath, TimeUtil.getToday(), task.getTaskId(), System.currentTimeMillis());
        LogUtil.info(log, task.getTaskId(), "init exe file: " + exeFile);
        File efile = new File(exeFile);
        if(!efile.getParentFile().exists()) efile.getParentFile().mkdirs();
        StringBuffer cmd = new StringBuffer(String.format("%s --name walrus_offline_job_%d ", header, task.getTaskId()));
        // add params
        if(StringUtils.isNotBlank(task.getParameter())) cmd.append(task.getParameter());
        // add run class
        cmd.append(String.format(" --class %s ", RUNNER_CLASS));
        // write resource
        cmd.append(String.format(" --driver-memory %s --num-executors %d --executor-memory %s --executor-cores %d ",
                resource.driverMem(), resource.executorNum(), resource.executorMem(), resource.coreNum()));
        // add jar path
        cmd.append(String.format(" %s ", RUNNER_JAR));
        // add zk
        cmd.append(String.format(" -zk.url %s -zk.timeout %d ", EncryptionUtil.base64Encode(zkUrl), zkTimeout));
        // add db info
        cmd.append(String.format(" -spring.datasource.url %s -spring.datasource.username %s -spring.datasource.password %s -spring.datasource.driver-class-name %s",
                EncryptionUtil.base64Encode(dbUrl), EncryptionUtil.base64Encode(dbUser), EncryptionUtil.base64Encode(dbPwd), EncryptionUtil.base64Encode(dbDriver)));
        // add job id params
        cmd.append(String.format(" -jobId %d ", task.getTaskId()));
        // write exe file
        cmds.add(cmd.toString());
        LogUtil.info(log, task.getTaskId(), "cmd: " + cmds);
        FileUtils.writeLines(efile, cmds);
        efile.setExecutable(true);
        // submit job
        ExecuteResultHandler resultHandler = new ExitHandler(task.getTaskId());
        CommandLine cmdLine = CommandLine.parse(exeFile);
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(new LogHandler());
        executor.setWatchdog(new ExecuteWatchdog(task.getTimeout() * 1000)); // timeout
        // run background
        LogUtil.info(log, task.getTaskId(), "submit cmd: " + Arrays.toString(cmdLine.toStrings()));
        executor.execute(cmdLine, resultHandler);
    }

    /**
     * check job status after execution finished
     */
    private class ExitHandler implements ExecuteResultHandler {

        private long taskId;

        public ExitHandler(long taskId) {
            this.taskId = taskId;
        }

        @Override
        public void onProcessComplete(int exitValue) {
            checkStatus(exitValue, null);
        }

        @Override
        public void onProcessFailed(ExecuteException e) {
            LogUtil.error(log, taskId, "exe error: " + ExceptionUtils.getFullStackTrace(e));
            checkStatus(1, e);
        }

        /**
         * if exitVal > 0, update job status to failed <br>
         * if job status is not failed nor success, update job status to failed <br>
         */
        private void checkStatus(int exitVal, ExecuteException e) {
            if(exitVal > 0) {
                String msg = e == null ? String.format("job %s Execute failed!", taskId) : ExceptionUtils.getFullStackTrace(e);
                LogUtil.error(log, taskId, msg);
                // fail task
                Task t = taskDao.getTask(taskId);
                if(t.getStatus() != CoreConstants.JOB_STATUS_SUCCESSED
                        && t.getStatus() != CoreConstants.JOB_STATUS_FAILED)
                    taskDao.fail(taskId, CoreConstants.JOB_STATUS_FAILED, WException.SYSTEM_ERROR, msg);
            } else log.info(String.format("job %s execute succeed!", taskId));
        }
    }

    /**
     * handle SPARK console log
     */
    private class LogHandler implements ExecuteStreamHandler {
        //TODO handle spark log
        @Override
        public void setProcessInputStream(OutputStream os) throws IOException {}
        @Override
        public void setProcessErrorStream(InputStream is) throws IOException {}
        @Override
        public void setProcessOutputStream(InputStream is) throws IOException {}
        @Override
        public void start() throws IOException {}

        @Override
        public void stop() throws IOException {}
    }
}

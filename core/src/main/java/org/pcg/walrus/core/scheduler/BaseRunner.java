package org.pcg.walrus.core.scheduler;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import org.pcg.walrus.common.util.ConfUtil;
import org.pcg.walrus.common.util.EncryptionUtil;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.execution.IExecutor;
import org.pcg.walrus.core.execution.WResult;
import org.pcg.walrus.core.execution.SparkExecutor;
import org.pcg.walrus.core.parse.ParserAgent;
import org.pcg.walrus.core.plan.LogicalPlan;
import org.pcg.walrus.core.plan.optimize.IOpitimizer;
import org.pcg.walrus.core.scheduler.rpc.RpcServer;
import org.pcg.walrus.core.scheduler.rpc.RpcServiceManager;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.exception.WException;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.common.io.ZkClient;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.util.RpcUtil;

import org.pcg.walrus.meta.WMeta;
import org.pcg.walrus.meta.pb.WJobMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * walrus job runner
 */
public abstract class BaseRunner implements Serializable {

    private static final long serialVersionUID = 4322184650727870781L;

    protected static final Logger log = LoggerFactory.getLogger(BaseRunner.class);

    // params
    protected Map<String, String> conf;
    // env
    protected WContext context;
    protected WMeta meta;
    protected String level;

    protected RpcServer server = new RpcServer();
    // io
    protected Connection connection;
    protected ZkClient client;

    // runner host
    private InetAddress address;
    private int port;

    public BaseRunner(String level) {
        this.level = level;
    }

    /**
     * 1 init walrus context
     * 2 load meta
     * 3 start job monitor for job listening
     * 4 register server to zk
     * 5 connect db
     * 6 process job|jobs
     */
    public void run(Map<String, String> conf) throws WServerException {
        this.conf = conf;
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.driver.maxResultSize", "0"); // driver data limit
        // ui memory usage limit
        sparkConf.set("spark.ui.retainedJobs", conf.getOrDefault("spark.ui.retainedJobs", "100"));
        sparkConf.set("spark.ui.retainedStages", conf.getOrDefault("spark.ui.retainedStages", "100"));
        sparkConf.set("spark.sql.ui.retainedExecutions", conf.getOrDefault("spark.ui.retainedExecutions", "100"));
        // spark config
        config(sparkConf);
        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
        // SET log LEVEL
        sparkSession.sparkContext().setLogLevel(conf.getOrDefault("spark.log.level", "INFO"));
        // https://issues.apache.org/jira/browse/SPARK-12837
        sparkSession.sqlContext().sql(String.format("SET spark.sql.autoBroadcastJoinThreshold = %s",
                conf.getOrDefault("spark.sql.autoBroadcastJoinThreshold", "524288000"))); // 500M, auto map join
        sparkSession.sparkContext().hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        // connect db ,zk
        try {
            // read conf priority:
            // 1 read from args
            // 2 read from properties file
            String propertiesFile = conf.getOrDefault("conf.file", "application.properties");
            Map<String, String> properties = ConfUtil.readProperties("/" + propertiesFile);
            String zkUrl = conf.containsKey("zk.url") ?
                    EncryptionUtil.base64Decode(conf.getOrDefault("zk.url", "")) :
                    properties.getOrDefault("zk.url", "");
            assert(StringUtils.isBlank(zkUrl)) : "zk url can not be found!";
            int timeout = Integer.parseInt(conf.getOrDefault("zk.timeout", "1000"));
            client = new ZkClient(zkUrl, timeout);
            client.connect();
            // connect to db
            String dbUrl = conf.containsKey("spring.datasource.url") ?
                    EncryptionUtil.base64Decode(conf.getOrDefault("spring.datasource.url", "")) :
                    properties.getOrDefault("spring.datasource.url", "");
            String user = conf.containsKey("spring.datasource.username") ?
                    EncryptionUtil.base64Decode(conf.getOrDefault("spring.datasource.username", "")) :
                    properties.getOrDefault("spring.datasource.username", "");
            String pwd = conf.containsKey("spring.datasource.password") ?
                    EncryptionUtil.base64Decode(conf.getOrDefault("spring.datasource.password", "")) :
                    properties.getOrDefault("spring.datasource.password", "");
            Class.forName(properties.getOrDefault("spring.datasource.driver-class-name", "com.mysql.jdbc.Driver"));
            connection = DriverManager.getConnection(dbUrl, user, pwd);
        } catch (ClassNotFoundException | SQLException | IOException e) {
            throw new WServerException("connect to db failed: " + e.getMessage());
        }
        // init context
        context = new WContext(sparkSession);
        String cacheLevel = conf.getOrDefault("cache.level", "disk");
        meta = new WMeta(context, client, level, cacheLevel, true);
        // load meta
        try {
            loadMeta();
        } catch (WMetaException e) {
            throw new WServerException("load meta failed: " + e.getMessage());
        }
        // start rpc server
        TaskMonitor monitor = loadMoniotor();
        address = RpcUtil.findLocalInetAddress();
        port = RpcUtil.availablePort(100);
        server.start(address, port, monitor);
        // registerServer to zk
        RpcServiceManager.registerRunner(client, level, address, port);
        // process
        process();
        // stop
        server.stop();
    }

    // load necessary meta from zk
    public abstract void config(SparkConf sparkConf);
    // load necessary meta from zk
    public abstract void loadMeta() throws WMetaException;
    // load necessary meta from zk
    public abstract TaskMonitor loadMoniotor() throws WServerException;
    // business process
    public abstract void process() throws WServerException;

    /**
     * run walrus job
     * 1 load job from db
     * 2 process job
     * 3 sink job to db
     */
    protected void runJob(long jobId) throws SQLException, WServerException {
        WJobMessage.WJob job = loadJob(jobId);
        // check job status
        if(job.getStatus() != CoreConstants.JOB_STATUS_WAITING)
            throw new WServerException(String.format("no need to run job with status: %d", job.getStatus()));
        WJobMessage.WJob.Builder builder = job.toBuilder();
        log.info("run job: " + job.getId());
        builder.setStatus(CoreConstants.JOB_STATUS_RUNNING);
        builder.setLevel(level);
        sink(builder.build());
        // register job on zk
        RpcServiceManager.registerJob(client, level, address, port, jobId);
        try {
            // job group
            String group = "WALRUS_JOB_" + jobId;
            context.getSc().setJobGroup(group, group, true);
            // parse job
            LogicalPlan plan = ParserAgent.parseJob(meta, level, jobId, job.getFormat(), job.getLogic());
            // optimize
            List<IOpitimizer> opitimizers = loadOpitimizers();
            for(IOpitimizer optimizer: opitimizers) {
                if(optimizer.apply()) {
                    log.info("job " + job.getId() + " optimize plan: " + optimizer.name());
                    optimizer.optimize(context, plan.getPlan());
                }
            }
            // save plan
            String jobPlan = plan.getTables() + "|" + plan.getDicts();
            builder.setJobPlan(jobPlan);
            // make sure table registered
            log.info("job " + job.getId() + " mkSure table Loaded: " + plan.getTables());
            meta.mkSureTableLoaded(plan.getTables());
            // make sure dict registered
            log.info("job " + job.getId() + " mkSure dict Loaded: " + plan.getDicts());
            meta.mkSureTableLoaded(plan.getDicts());
            // execute
            WResult result = execute(context, plan, job);
            // success job
            builder.setStatus(CoreConstants.JOB_STATUS_SUCCESSED);
            builder.setResultLines(result.getLines());
            builder.setSaveMode(result.getSaveMode());
            builder.setSavePath(result.getSavePath());
        } catch (WCoreException | WMetaException e) {
            int errorCode = e != null ? e.getErrorCode() : WException.SYSTEM_ERROR;
            String msg = e != null && e.getMessage() != null ? e.getMessage() : "System Error";
            LogUtil.error(log, job.getId(), ExceptionUtils.getFullStackTrace(e));
            // set fail
            builder.setErrorCode(errorCode);
            builder.setMessage(msg);
            builder.setStatus(CoreConstants.JOB_STATUS_FAILED);
        } catch (Exception e) {
            String msg = e != null && e.getMessage() != null ? e.getMessage() : "System Error";
            LogUtil.error(log, job.getId(), ExceptionUtils.getFullStackTrace(e));
            // set fail
            builder.setErrorCode(WException.SYSTEM_ERROR);
            builder.setMessage(msg);
            builder.setStatus(CoreConstants.JOB_STATUS_FAILED);
        } finally {
            // sink job to db
            sink(builder.build());
            // remove job on zk
            RpcServiceManager.unRegisterJob(client, jobId);
        }
    }

    // set job status fail
    protected void failJob(long jobId, int errorCode, String msg) {
        String sql = String.format("UPDATE %s set f_status=?,f_error_code=?,f_message=? WHERE f_task_id=%d", CoreConstants.DB_TASK_TABLE, jobId);
        try {
            PreparedStatement preparedStmt = connection.prepareStatement(sql);
            preparedStmt.setInt(1, CoreConstants.JOB_STATUS_FAILED);
            preparedStmt.setInt(2, errorCode);
            preparedStmt.setString(3, msg);
            preparedStmt.executeUpdate();
        } catch (SQLException e) {
            log.error("fail job error: " + e.getMessage());
        }
    }

    // load job from db
    protected WJobMessage.WJob loadJob(long jobId) throws SQLException {
        String sql = String.format("SELECT f_task_id,IFNULL(f_author,'') f_author,IFNULL(f_level,'') f_level,IFNULL(f_format,'') f_format,IFNULL(f_logic,'') f_logic,IFNULL(f_table,'') f_table,IFNULL(f_bday,'') f_bday,IFNULL(f_eday,'') f_eday,IFNULL(f_save_mode,'') f_save_mode,IFNULL(f_save_format,'') f_save_format,IFNULL(f_save_path,'') f_save_path,IFNULL(f_submit_type,'') f_submit_type,IFNULL(f_ext,'') f_ext,f_status FROM %s WHERE f_task_id=%s", CoreConstants.DB_TASK_TABLE, jobId);
        WJobMessage.WJob.Builder builder = WJobMessage.WJob.newBuilder();
        ResultSet result = connection.createStatement().executeQuery(sql);
        log.info(String.format("load job sql: %s", sql));
        if(result.next()) {
            builder.setId(result.getInt("f_task_id"));
            builder.setAuthor(result.getString("f_author"));
            builder.setLevel(result.getString("f_level"));
            builder.setFormat(result.getString("f_format"));
            builder.setLogic(result.getString("f_logic"));
            builder.setTable(result.getString("f_table"));
            builder.setBday(result.getString("f_bday"));
            builder.setEday(result.getString("f_eday"));
            builder.setSaveMode(result.getString("f_save_mode"));
            builder.setSaveFormat(result.getString("f_save_format"));
            builder.setSavePath(result.getString("f_save_path"));
            builder.setSubmitType(result.getString("f_submit_type"));
            builder.setStatus(result.getInt("f_status"));
            builder.setExt(result.getString("f_ext"));
        }
        return builder.build();
    }

    // update job on db
    protected void sink(WJobMessage.WJob job) throws SQLException {
        String sql = String.format("UPDATE %s SET f_time_end=CURRENT_TIMESTAMP, f_status=?, f_save_mode=?, f_save_path=?, f_result_lines=?, f_error_code=?, f_message=?, f_plan=? WHERE f_task_id=%d", CoreConstants.DB_TASK_TABLE, job.getId());
        PreparedStatement preparedStmt = connection.prepareStatement(sql);
        log.info(String.format("sink job sql: %s", sql));
        preparedStmt.setInt(1, job.getStatus());
        preparedStmt.setString(2, job.getSaveMode());
        preparedStmt.setString(3, job.getSavePath());
        preparedStmt.setLong(4, job.getResultLines());
        preparedStmt.setInt(5, job.getErrorCode());
        preparedStmt.setString(6, job.getMessage());
        preparedStmt.setString(7, job.getJobPlan());
        preparedStmt.executeUpdate();
    }

    /**
     * calculation engine:
     * 		currently, only spark is supported
     */
    private WResult execute(WContext context, LogicalPlan plan, WJobMessage.WJob job) throws WCoreException {
        IExecutor executor = null;
        switch (plan.getParams().getOrDefault("execute.engine", "spark").toString()) {
            case "spark":
            default:
                executor = new SparkExecutor();
        }
        return executor.execute(context, plan, job);
    }

    // load all plan opitimizer
    private List<IOpitimizer> loadOpitimizers() throws Exception {
        List<IOpitimizer> opitimizers = new ArrayList<>();
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final String WALRUS_OPTIMIZE_PATH = "org.pcg.walrus.core.plan.optimize";
        ImmutableSet<ClassPath.ClassInfo> clazz = ClassPath.from(loader).getTopLevelClasses(WALRUS_OPTIMIZE_PATH);
        for (final ClassPath.ClassInfo c : clazz) {
            final Class<?> opitimizer =  c.load();
            if(!opitimizer.isInterface()) {
                IOpitimizer f = (IOpitimizer) Class.forName(opitimizer.getName()).newInstance();
                opitimizers.add(f);
            }
        }
        return opitimizers;
    }

}


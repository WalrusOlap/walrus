package org.pcg.walrus.core.scheduler;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.pcg.walrus.common.util.ConfUtil;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.pb.WJobMessage;
import org.pcg.walrus.common.util.TimeUtil;

import java.sql.SQLException;

/**
 * walrus job offline runner: spark submit task
 */
public class OfflineRunner extends BaseRunner {

    public OfflineRunner(String level) {
        super(level);
    }

    @Override
    public void config(SparkConf sparkConf) {
        // TODO common spark config
    }

    @Override
    public void loadMeta() throws WMetaException {
        try {
            long jobId = getJobId();
            WJobMessage.WJob job = loadJob(jobId);
            String table = job.getTable();
            String bday = job.getBday();
            String eday = job.getEday();
            assert(StringUtils.isBlank(table)) : CoreConstants.JSON_KEY_TABLE + " must be set!";
            assert(StringUtils.isBlank(bday)) : CoreConstants.JSON_KEY_BDAY + " must be set!";
            assert(StringUtils.isBlank(eday)) : CoreConstants.JSON_KEY_EDAY + " must be set!";
            meta.loadView(table, TimeUtil.stringToDate(bday), TimeUtil.stringToDate(eday), false);
        } catch (Exception e) {
            log.error("load meta error: " + ExceptionUtils.getFullStackTrace(e));
            throw new WMetaException("load meta error: " + e.getMessage());
        }
    }

    @Override
    public TaskMonitor loadMoniotor() {
        return new TaskMonitor() {
            @Override
            public void run(long jobId) {
                // do nothing
            }

            @Override
            public void kill(long jobId) {
                context.getSc().cancelJobGroup("WALRUS_JOB_" + jobId);
                context.getSc().stop();
            }
        };
    }

    @Override
    public void process() throws WServerException {
        long jobId = getJobId();
        try {
            runJob(jobId);
        } catch (SQLException e) {
            throw new WServerException("run job error: " + e.getMessage());
        }
    }

    // load job
    private long getJobId() throws WServerException {
        if(!conf.containsKey("jobId")) throw new WServerException("No job found!");
        long jobId = Integer.parseInt(conf.get("jobId"));
        return jobId;
    }

    // job entrance
    public static void main(String[] args) throws Exception {
        new OfflineRunner(MetaConstants.CLUSTER_LEVEL_OFFLINE).run(ConfUtil.parseArgs(args));
    }
}

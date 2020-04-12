package org.pcg.walrus.core.scheduler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.pcg.walrus.common.exception.WException;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.common.util.ConfUtil;
import org.pcg.walrus.meta.MetaConstants;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * resident server to listening for online jobs
 */
public class OnlineRunner extends BaseRunner {

    private LinkedBlockingQueue<Long> _q_jobs;

    public OnlineRunner(String level) {
        super(level);
        _q_jobs = new LinkedBlockingQueue<Long>();
    }

    @Override
    public void config(SparkConf sparkConf) {
        // TODO common spark config
    }

    @Override
    public void loadMeta() throws WMetaException {
        meta.load();
    }

    @Override
    public TaskMonitor loadMoniotor() throws WServerException {
        return new TaskMonitor() {

            @Override
            public void run(long jobId) {
                try {
                    _q_jobs.put(jobId);
                } catch (InterruptedException e) {
                    log.error("put job to queue error: " + e.getMessage());
                    failJob(jobId, WException.SYSTEM_ERROR, e.getMessage());
                }
            }

            @Override
            public void kill(long jobId) {
                context.getSc().cancelJobGroup("WALRUS_JOB_" + jobId);
            }
        };
    }

    @Override
    public void process() throws WServerException {
        int threads = Integer.parseInt(conf.getOrDefault("worker.threads", "3"));
        ExecutorService workers = Executors.newFixedThreadPool(threads);
        for(int i = 0; i < threads; i++)  workers.execute(new Thread() {
            @Override
            public void run() {
                while (true) {
                    long job = 0;
                    try {
                        job = _q_jobs.take();
                        runJob(job);
                    } catch (Exception e) {
                        failJob(job, WException.SYSTEM_ERROR, "run job error: " + e.getMessage());
                        log.error("run job error: " + ExceptionUtils.getFullStackTrace(e));
                    }
                }
            }
        });
    }

    // job entrance
    public static void main(String[] args) throws Exception {
        new OnlineRunner(MetaConstants.CLUSTER_LEVEL_ONLINE).run(ConfUtil.parseArgs(args));
    }
}

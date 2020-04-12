package org.pcg.walrus.core.execution.sink;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.core.execution.WResult;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.meta.pb.WJobMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsSinker implements ISinker {

    private static final Logger log = LoggerFactory.getLogger(HdfsSinker.class);

    private static final int DEFAULT_PARTITION_SIZE = 1000000;

    @Override
    public void sink(WContext context, Dataset<Row> df, WJobMessage.WJob job, WResult result) throws WCoreException {
        // get extra parameter
        JSONObject conf = new JSONObject();
        try {
            conf = (JSONObject) new JSONParser().parse(job.getExt());
        } catch (ParseException e) {
            // do nothing
        }
        String format = job.getSaveFormat();
        String defaultPath = String.format("%s/%s/%d_%s", CoreConstants.DEFAULT_HDFS_PATH,
                TimeUtil.getToday(), job.getId(), System.currentTimeMillis());
        String sinkPath = StringUtils.isBlank(job.getSavePath()) ? defaultPath : job.getSavePath();
        result.setSavePath(sinkPath);
        // repartition
        DataFrameWriter dfw = repartition(df, result.getLines(), conf, job.getId()).write();
        // save
        switch (format.toLowerCase()) {
            case MetaConstants.META_FORMAT_PARQUET:
                LogUtil.info(log, job.getId(), "save to(parquet): " + sinkPath);
                dfw.mode(SaveMode.Overwrite).parquet(sinkPath);
                break;
            case MetaConstants.META_FORMAT_CSV:
            default:
                LogUtil.info(log, job.getId(), "save to(csv): " + sinkPath);
                String delimiter = conf.getOrDefault("sink.delem", "\t").toString();
                dfw.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
                        .option("header", conf.getOrDefault("s", "false").toString())
                        .option("delimiter", delimiter)
                        .option("quoteMode", "NONE")
                        .option("escape", "\\")
                        .save(sinkPath);
                break;
        }
    }

    /**
     * change output file num
     */
    private Dataset<Row> repartition(Dataset<Row> dataframe, long lines, JSONObject conf, long jobId) {
        if(lines < 1) return dataframe.repartition(1);
        int partition = (int) (lines / DEFAULT_PARTITION_SIZE + 1);
        if(conf.containsKey("partition.num"))
            partition = Integer.parseInt(String.valueOf(conf.get("partition.num")));
        LogUtil.info(log, jobId, "data[" + lines + "] repatition to " + partition);
        return dataframe.repartition(partition);
    }
}

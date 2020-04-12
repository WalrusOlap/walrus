package org.pcg.walrus.core.execution.job.stage;

import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.query.RPartition;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.io.ClickHouseClient;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.pcg.walrus.meta.pb.WJobMessage;

/**
 * ! do not directly query ck on driver
 * TODO if query data is too big, better to query each ch node and do aggregation in spark
 */
public class CkStage extends SqlStage {

    private static final long serialVersionUID = -2713531310606626293L;

    public CkStage(WContext context, RPartition partition, Operator operator) {
        super(context, partition, operator);
    }

    @Override
    public Dataset<Row> exe(WJobMessage.WJob job) throws WCoreException {
        // parse json
        JSONObject json = null;
        String path = partition.getPath();
        try {
            json = (JSONObject) new JSONParser().parse(path);
        } catch (ParseException e) {
            log.error("parse json error: " + ExceptionUtils.getFullStackTrace(e));
            throw new WCoreException("Invalid data path: " + path);
        }
        String sql = generateSql((String) json.get("table"));
        LogUtil.info(log, job.getId(), "query ck: " + sql);
        String url = json.getOrDefault("url","").toString();
        String user = json.getOrDefault("user", "").toString();
        String password = json.getOrDefault("password", "").toString();
        int timeout = Integer.parseInt(json.getOrDefault("timeout", "1000").toString());
        Dataset<Row> rows = ClickHouseClient.query(context.getSession(), sql,
                json.getOrDefault("ip","").toString(),
                Integer.parseInt(json.getOrDefault("port","0").toString()),
                json.getOrDefault("db","").toString(), user, password, timeout);
        LogUtil.info(log, job.getId(), "ck return: " + rows.schema());
        return rows;
    }
}

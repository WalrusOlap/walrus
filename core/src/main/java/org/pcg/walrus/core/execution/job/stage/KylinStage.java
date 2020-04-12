package org.pcg.walrus.core.execution.job.stage;

import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.io.KylinClient;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.query.RPartition;
import org.pcg.walrus.meta.pb.WJobMessage;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * ! do not directly query kylin on driver
 */
public class KylinStage extends SqlStage {

    public KylinStage(WContext context, RPartition partition, Operator operator) {
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
            throw new WCoreException("Invalid kylin path: " + path);
        }
        String sql = generateSql(json.getOrDefault("table", "").toString());
        String url = (String) json.get("url");
        String user = (String) json.get("user");
        String pwd = (String) json.get("pwd");
        LogUtil.info(log, job.getId(), "query kylin: " + sql);
        Dataset<Row> rows = KylinClient.query(context.getSession(), sql, url, user, pwd);
        LogUtil.info(log, job.getId(), "query kylin return: " + rows.schema());
        return rows;
    }

}

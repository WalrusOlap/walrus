package org.pcg.walrus.core.execution.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.core.execution.WResult;
import org.pcg.walrus.meta.pb.WJobMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;
import java.util.HashMap;

/**
 * save to db
 */
public class JdbcSinker implements ISinker {

    private static final Logger log = LoggerFactory.getLogger(JdbcSinker.class);

    private static Map<String,String> columnMap = new HashMap<String,String>();

    @Override
    public void sink(WContext context, Dataset<Row> df, WJobMessage.WJob job, WResult result) throws WCoreException {
        // save info
        JSONObject dbInfo = null;
        try {
            dbInfo = (JSONObject) new JSONParser().parse(job.getSavePath());
        } catch (ParseException e) {
            throw new WCoreException(e.getMessage());
        }
        String url = dbInfo.getOrDefault("url", "").toString();
        String table = dbInfo.getOrDefault("table", "").toString();
        String user = dbInfo.getOrDefault("user", "").toString();
        String driver = dbInfo.getOrDefault("driver", "com.mysql.jdbc.Driver").toString();
        String password = dbInfo.getOrDefault("password", "").toString();
        // create table if necessary
        if(table.length() == 0) {
            table = String.format("t_walrus_result_%d", job.getId());
            dbInfo.put("table", table);
        }
        // save mysql
        df.write().format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .save();
        // update result
        result.setSavePath(dbInfo.toJSONString());
    }

}


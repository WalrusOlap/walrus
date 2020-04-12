package org.pcg.walrus.meta.register.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.common.io.JdbcClient;
import org.pcg.walrus.meta.pb.WTableMessage;

/**
 * read jdbc data
 */
public class SourceDb implements ISource {

    @Override
    public Dataset<Row> register(WContext context, WTableMessage.WTablet tablet) throws WMetaException {
        JSONObject dbInfo = null;
        try {
            dbInfo = (JSONObject) new JSONParser().parse(tablet.getPath());
        } catch (ParseException e) {
            throw new WMetaException(e.getMessage());
        }
        return JdbcClient.readMysql(context.getSqlContext(),
                dbInfo.getOrDefault("url", "").toString(),
                dbInfo.getOrDefault("table", "").toString(),
                dbInfo.getOrDefault("user", "").toString(),
                dbInfo.getOrDefault("password", "").toString(),
                tablet.getSchema(), "1=1", "");
    }
}

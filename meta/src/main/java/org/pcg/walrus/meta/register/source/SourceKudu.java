package org.pcg.walrus.meta.register.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.meta.pb.WTableMessage;

/**
 * read kudu
 */
public class SourceKudu implements ISource {

    @Override
    public Dataset<Row> register(WContext context, WTableMessage.WTablet tablet) throws WMetaException {
        JSONObject kuduInfo = null;
        try {
            kuduInfo = (JSONObject) new JSONParser().parse(tablet.getPath());
        } catch (ParseException e) {
            throw new WMetaException(e.getMessage());
        }
        return context.getSqlContext().read()
                .option("kudu.master", kuduInfo.get("master").toString())
                .option("kudu.table", kuduInfo.get("table").toString())
                .format("kudu").load();
    }

}

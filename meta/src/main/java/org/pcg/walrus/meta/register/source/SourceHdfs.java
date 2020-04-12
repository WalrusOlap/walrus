package org.pcg.walrus.meta.register.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.common.io.HdfsJavaReader;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.pb.WTableMessage;

/*
 * register data on hdfs
 */
public class SourceHdfs implements ISource {

    @Override
    public Dataset<Row> register(WContext context, WTableMessage.WTablet t) throws WMetaException {
        Dataset<Row> df = null;
        String path = t.getPath();
        switch (t.getFormat().toLowerCase()) {
            case MetaConstants.META_FORMAT_PARQUET:
                df = context.getSqlContext().read().parquet(path);
                break;
            case MetaConstants.META_FORMAT_ORC:
                df = context.getSqlContext().read().orc(path);
                break;
            case MetaConstants.META_FORMAT_JSON:
                df = context.getSqlContext().read().json(path);
                break;
            case MetaConstants.META_FORMAT_CSV:
            default:
                try {
                    df = HdfsJavaReader.loadCsv(context.getJSc(), context.getSqlContext(), path,
                            t.getSchema(), t.getDelim(), t.getRecordNum(), t.getFileNum());
                } catch (Exception e) {
                    throw new WMetaException(e.getMessage());
                }
                break;
        }
        return df;
    }

}

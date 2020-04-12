package org.pcg.walrus.meta.register.source;

import org.pcg.walrus.meta.MetaConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * source factory
 */
public class SourceFactory {

    private static Map<String, ISource> sources = new HashMap<>();

    // init sources
    static {
        ISource hdfs = new SourceHdfs();
        sources.put(MetaConstants.META_FORMAT_CSV, hdfs);
        sources.put(MetaConstants.META_FORMAT_PARQUET, hdfs);
        sources.put(MetaConstants.META_FORMAT_ORC, hdfs);
        sources.put(MetaConstants.META_FORMAT_JSON, hdfs);

        ISource db = new SourceDb();
        sources.put(MetaConstants.META_FORMAT_JDBC, db);
        sources.put(MetaConstants.META_FORMAT_MYSQL, db);
        sources.put(MetaConstants.META_FORMAT_DB, db);

        ISource kudu = new SourceKudu();
        sources.put(MetaConstants.META_FORMAT_KUDU, kudu);

        ISource tdw = new SourceTdw();
        sources.put(MetaConstants.META_FORMAT_TDW, tdw);
    }

    /**
     * load source based on format
     */
    public static ISource loadSource(String format) {
        return sources.getOrDefault(format, new SourceLazy());
    }
}

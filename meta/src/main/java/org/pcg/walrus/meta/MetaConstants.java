package org.pcg.walrus.meta;

/**
 * meta constants class
 */
public class MetaConstants {

    // env
    public static final String CLUSTER_LEVEL_ALL = "ALL";
    public static final String CLUSTER_LEVEL_ONLINE = "online";
    public static final String CLUSTER_LEVEL_OFFLINE = "offline";

    // zk
    public static final String ZK_PATH_ROOT = "/walrus";
    public static final String ZK_PATH_META = ZK_PATH_ROOT + "/meta";
    public static final String ZK_PATH_UPDATE = ZK_PATH_META + "/update";
    public static final String ZK_PATH_RPC = ZK_PATH_ROOT + "/rpc";
    public static final String ZK_PATH_JOB = ZK_PATH_ROOT + "/job";
    public static final String ZK_PATH_LOCK = ZK_PATH_ROOT + "/lock";

    public static final String ZK_PATH_META_VIEW = ZK_PATH_META + "/view";
    public static final String ZK_PATH_META_TABLE = ZK_PATH_META + "/table";
    public static final String ZK_PATH_META_DICT = ZK_PATH_META + "/dict";

    // time
    public static int DEFAULT_BDAY = 19970101;
    public static int DEFAULT_EDAY = 20501231;

    // meta name
    public final static String META_DELIM_NAME = "__";

    // meta format
    public final static String META_FORMAT_PARQUET = "parquet";
    public final static String META_FORMAT_CSV = "csv";
    public final static String META_FORMAT_KUDU = "kudu";
    public final static String META_FORMAT_DRUID = "druid";
    public final static String META_FORMAT_JDBC = "jdbc";
    public final static String META_FORMAT_DB = "db";
    public final static String META_FORMAT_MYSQL = "mysql";
    public final static String META_FORMAT_KYLIN = "kylin";
    public final static String META_FORMAT_CK = "clickhouse";
    public final static String META_FORMAT_ES = "es";
    public final static String META_FORMAT_ORC = "orc";
    public final static String META_FORMAT_JSON = "json";
    public final static String META_FORMAT_TDW = "tdw";

    // partition mode
    public static final String PARTITION_MODE_A = "A"; // partitioned by all data
    public static final String PARTITION_MODE_Y = "Y"; // partitioned by year
    public static final String PARTITION_MODE_M = "M"; // partitioned by month
    public static final String PARTITION_MODE_D = "D"; // partitioned by date
    public static final String PARTITION_MODE_H = "H"; // partitioned by hour

    // meta field
    public final static String META_FIELD_METRIC = "metric";
    public final static String META_FIELD_DIM = "dimension";
    public final static String META_TABLE_TYPE_T = "table";
    public final static String META_TABLE_TYPE_D = "dict";
    public final static String META_BASE_TABLE_NAME = "base";

    // meta status
    public final static int META_STATUS_VALID = 1;
    public final static int META_STATUS_INVALID = 0;
    public final static int TABLE_PRIORITY_DEFAULT = 0;
}

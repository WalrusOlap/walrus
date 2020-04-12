package org.pcg.walrus.core;

public class CoreConstants {

    // operator
    public final static String AST_OPER_SELECT = "SELECT";
    public final static String AST_OPER_UNION = "UNION";
    public final static String AST_OPER_AGGREGATOR = "AGGREGATION";
    public final static String AST_OPER_COMBINER = "COMBINER";

    // query
    public final static String DERIVE_MODE_SELECT = "select";
    public final static String DERIVE_MODE_JOIN = "join";
    public final static String DERIVE_MODE_EXTEND = "extend";
    public final static String DERIVE_MODE_VIRTUAL = "virtual";
    public final static String DERIVE_MODE_EXT1 = "ext1"; // derive mode of join field is extend
    public final static String DERIVE_MODE_EXT2 = "ext2"; // derive mode of join field is join
    public final static String QUERY_METRIC_DEFAULT_COUNT = "default_count";
    public final static String OPER_UNION_SIMPLE = "simple";
    public final static String OPER_UNION_SUPPLEMENT = "supplement";
    public final static String OPER_UNION_SPLIT = "split";

    // function
    public final static String METRIC_FUNCTION_SUM = "sum";
    public final static String METRIC_FUNCTION_COUNT = "count";
    public final static String METRIC_FUNCTION_MAX = "max";
    public final static String METRIC_FUNCTION_MIN = "min";
    public final static String METRIC_FUNCTION_AVG = "avg";
    public final static String METRIC_FUNCTION_COLLECT = "COLLECT_LIST";
    public final static String METRIC_FUNCTION_DISTINCT = "COUNT_DISTINCT";
    public final static String METRIC_FUNCTION_FREQ = "FREQ";
    public final static String METRIC_FUNCTION_MATH_EXP = "math_expression";
    public final static String UDF_METHOD_DEFAULT = "DEFAULT_METHOD";
    public final static String UDF_CLASS_DEFAULT = "DEFAULT_CLASS";

    // sql
    public final static String SQL_FACT_TABLE_ALIAS = "rf_alias";
    public final static String EXT_TYPE_JOIN = "join";
    public final static String TYPE_JOIN_LEFT = "left";

    // json
    public final static String JSON_KEY_TABLE = "table";
    public final static String JSON_KEY_BDAY = "begin_day";
    public final static String JSON_KEY_EDAY = "end_day";
    public final static String JSON_KEY_WHERE = "where";
    public final static String JSON_KEY_COLS = "group_by";
    public final static String JSON_KEY_MS = "metric";
    public final static String JSON_KEY_ORDERBY = "order_by";
    public final static String JSON_KEY_LIMIT = "limit";
    public final static String JSON_DELIM = ",";

    // schema
    public final static String COLUMN_TYPE_INT = "INT";
    public final static String COLUMN_TYPE_DOUBLE = "DOUBLE";
    public final static String COLUMN_TYPE_TEXT = "TEXT";
    public final static String COLUMN_TYPE_STRING = "STRING";
    public final static String COLUMN_TYPE_CHARARRAY = "CHARARRAY";
    public final static String COLUMN_TYPE_LONG = "LONG";
    public final static String COLUMN_TYPE_CHAR = "CHAR";
    public final static String COLUMN_TYPE_NCHAR = "NCHAR";
    public final static String COLUMN_TYPE_VARCHAR = "VARCHAR";
    public final static String COLUMN_TYPE_DATE = "DATE";
    public final static String COLUMN_TYPE_TIMESTAMP = "TIMESTAMP";

    public final static String COLUMN_TYPE_DECIMAL = "DECIMAL";
    public final static String COLUMN_TYPE_NUMBER = "NUMBER";
    public final static String COLUMN_TYPE_DOUBLE_PRECISION = "DOUBLE PRECISION";

    public final static String COLUMN_TYPE_TINYINT = "TINYINT";
    public final static String COLUMN_TYPE_SMALLINT = "SMALLINT";
    public final static String COLUMN_TYPE_BIGINT = "BIGINT";
    public final static String COLUMN_TYPE_BYTEA = "BYTEA";
    // parse
    public final static String PARSER_TPYE_JSON = "json";
    public final static String PARSER_TPYE_SQL = "sql";
    public final static String PARTITION_IGNORE_TYPE_EXCELUSIVE = "exclusive";
    public final static String PARTITION_IGNORE_TYPE_EQ = "eq";
    public final static String PARTITION_IGNORE_TYPE_NEQ = "neq";
    public final static String PARTITION_IGNORE_TYPE_VIOLENCE = "violence";
    public final static int TABLE_PRIORITY_DEFAULT = 0;

    // DB
    public final static String DB_TASK_TABLE = "t_walrus_task";

    // sink
    public final static String DEFAULT_HDFS_PATH = "/user/walrus/result";
    public final static String SAVE_MODE_HDFS = "hdfs";
    public final static String SAVE_MODE_DB = "db";
    public final static String SAVE_MODE_MYSQL = "mysql";
    public final static String SAVE_MODE_CK = "ck";

    // status
    public final static int JOB_STATUS_WAITING = 1;
    public final static int JOB_STATUS_RUNNING = 2;
    public final static int JOB_STATUS_SUCCESSED = 3;
    public final static int JOB_STATUS_FAILED = 4;
    public final static int JOB_STATUS_TOKILL = 5;
    public final static int JOB_STATUS_KILLING = 6;
    public final static int JOB_STATUS_KILLED = 7;
}

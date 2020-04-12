package org.pcg.walrus.core.execution.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.types.StructField;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.io.ClickHouseClient;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.execution.WResult;
import org.pcg.walrus.meta.pb.WJobMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sink result to db: cdb, clickhouse...
 */
public class DbSinker implements ISinker {

    private static final Logger log = LoggerFactory.getLogger(DbSinker.class);

    private static final int DEFAULT_PARTITION_SIZE = 1000000;
    private static final int SIZE_LIMIT = 10000;
    private static final String DRIVER_MYSQL = "com.mysql.jdbc.Driver";
    private static final String DRIVER_CLICKHOUSE = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final String DRIVER_DEFAULT = DRIVER_MYSQL;
    private static Map<String,Boolean> createTableMap = new HashMap();
    private static Map<String,String> sqlFmtMap = new HashMap();
    private static Map<String,Integer> partitionSizeMap = new HashMap();
    private static Map<String,Integer> batchSizeMap = new HashMap();
    private static Map<String,Map<String,String>> columnMap = new HashMap();

    private static final String CREATE_STATEMENT_MYSQL = "CREATE TABLE IF NOT EXISTS %s.`%s` ( `f_id` INT UNSIGNED AUTO_INCREMENT, %s, PRIMARY KEY ( `f_id` ) ) ENGINE=InnoDB DEFAULT CHARSET=utf8";

    static {
        init();
    }

    /**
     * init
     */
    private static void init() {
        createTableMap.put(DRIVER_MYSQL, true);
        createTableMap.put(DRIVER_CLICKHOUSE, false);
        sqlFmtMap.put(DRIVER_MYSQL, CREATE_STATEMENT_MYSQL);
        partitionSizeMap.put(DRIVER_MYSQL, 20000);
        partitionSizeMap.put(DRIVER_CLICKHOUSE, 1000000);
        batchSizeMap.put(DRIVER_MYSQL, 1000);
        batchSizeMap.put(DRIVER_CLICKHOUSE, 100000);
        Map<String,String> mysqlColumnMap = new HashMap();
        mysqlColumnMap.put("integer", "int(11) DEFAULT 0");
        mysqlColumnMap.put("long", "BIGINT");
        mysqlColumnMap.put("double", "double DEFAULT 0.0");
        mysqlColumnMap.put("decimal", "double DEFAULT 0.0");
        mysqlColumnMap.put("default", "TEXT DEFAULT NULL");
        columnMap.put(DRIVER_MYSQL, mysqlColumnMap);
    }
    @Override
    public void sink(WContext context, Dataset<Row> df, WJobMessage.WJob job, WResult result) throws WCoreException {
        // get extra parameter
        JSONObject dbInfo = null;
        try {
            dbInfo = (JSONObject) new JSONParser().parse(job.getSavePath());
        } catch (ParseException e) {
            throw new WCoreException(e.getMessage());
        }

        // create if necessary
        String table = dbInfo.containsKey("table") ? (String) dbInfo.get("table") : String.format("walrus_table_%s_%s", job.getId(), System.currentTimeMillis());
        String driver = dbInfo.containsKey("driver") ? (String) dbInfo.get("driver") : DRIVER_DEFAULT;
        createOrClearTable(dbInfo, driver, dbInfo.get("db").toString(), table, df, job);
        String saveMode = job.getSaveMode();
        switch (saveMode) {
            case CoreConstants.SAVE_MODE_MYSQL:
                //! spark 2.1 jdbc save is not compatible with cdb5.6, so save to mysql manually
                List<String> schemas = new ArrayList();
                for(StructField field:  df.schema().fields()) schemas.add("`" + field.name() + "`");
                String schema = Joiner.on(",").join(schemas);
                // save db
                int partitionNum = Integer.parseInt(dbInfo.get("partition.num").toString());
                saveToMysql(dbInfo, DRIVER_MYSQL, table,
                        repartition(df, result.getLines(), partitionNum, getFromMapByDefault(partitionSizeMap, driver), job.getId()),
                        schemas, job.getId());
                dbInfo.put("schema", schema);
                break;
            case CoreConstants.SAVE_MODE_CK:
                // insert
                ClickHouseClient.insert(df, context.getSession(), (String) dbInfo.get("servers"), table,
                        (String) dbInfo.get("db"), (String) dbInfo.get("user"),
                        (String) dbInfo.get("pwd"), 1000, 1, 60);
                break;
             default: break;
        }
        dbInfo.put("table", table);
        result.setSavePath(dbInfo.toJSONString());
    }

    /**
     * 1 if mode overwrite drop table
     * 2 create table if not exist
     * @throws WCoreException
     */
    protected static void createOrClearTable(JSONObject json, String driver, String db, String table, Dataset<Row> dataframe, WJobMessage.WJob job) throws WCoreException {
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection((String)json.get("url"), (String) json.get("user"), (String) json.get("pwd"));
            Statement stmt = conn.createStatement();
            // create table
            String needCreate = (String) json.getOrDefault("need_create", "Y");
            if ("Y".equalsIgnoreCase(needCreate) && getFromMapByDefault(createTableMap, driver) ) {
                String sql = buildCreateTableSql(db, table, dataframe.schema().fields(), driver, job.getId());
                stmt.executeUpdate(sql.toString());
            }
            String mode = json.containsKey("mode") ? (String) json.get("mode") : "overwrite";
            String date = json.containsKey("date") ? (String) json.get("date") : "0";
            String hourStr = json.containsKey("hour") ? (String) json.get("hour") : "-1";
            String isDateRange = json.containsKey("is_date_range") ? String.valueOf(json.get("is_date_range")) : "0" ;
            int hour = -1;
            try {
                hour = Integer.parseInt(hourStr);
            } catch (NumberFormatException e) {
                log.error("parse hour error: " + e.getMessage());
            }
            // clear table base on save mode
            switch (mode) {
                case "overwrite":
                    String drop = String.format("DELETE FROM %s.%s", db,table);
                    log.info("exe drop sql: " + drop);
                    int result = stmt.executeUpdate(drop);
                    if(result == -1) throw new WCoreException("drop table " + table + " failed: " + drop);
                    break;
                case "ch-partition":
                    String partition = date;
                    if(hour > -1) partition = date + "," + hour;
                    log.info("drop partition: " + partition);
                    if("1".equals(isDateRange)) {
                        partition = "";
                        try {
                            Date[] days = TimeUtil.getPartitonDays(TimeUtil.stringToDate(job.getBday()),TimeUtil.stringToDate(job.getEday()));
                            if(days != null ) {
                                for (Date datePartition : days) {
                                    if(datePartition == null) continue;
                                    if(partition.length() > 0) partition = partition + ";";
                                    partition = partition + TimeUtil.DateToString(datePartition);
                                }
                            }
                        } catch (Exception e) {
                            log.error("parse date exception : "+ e.getMessage());
                        }
                    }
                    log.info("exe delete partition : " + partition);
                    ClickHouseClient.dropPartition((String) json.get("servers"), table, partition, db, (String) json.get("user"), (String) json.get("pwd"), 1000);
                    break;
                case "append":
                    String delelte = String.format("DELETE FROM %s.%s  WHERE date=%s ", db,table,date);
                    if(hour > -1) delelte = delelte + " AND hour=" + hour;
                    // if delete data by begin_date/end_date in logic
                    if("1".equals(isDateRange)) {
                        String beginDay = job.getBday();
                        String endDay = job.getEday();
                        try {
                            beginDay = TimeUtil.DateToString(TimeUtil.stringToDate(beginDay));
                            endDay = TimeUtil.DateToString(TimeUtil.stringToDate(endDay));
                            if (beginDay != null && endDay != null) {
                                delelte = String.format("DELETE FROM %s.%s  WHERE date between %s and %s ", db,table,beginDay,endDay);
                            }
                        } catch (Exception e) {
                            log.error("parse date exception : "+ e.getMessage());
                        }
                    }
                    log.info("exe delete sql: " + delelte);
                    int delRet = stmt.executeUpdate(delelte);
                    if(delRet == -1) throw new WCoreException("delete table " + table + " failed: " + delelte);
                    break;
                default:
                    break;
            }
        } catch (SQLException | ClassNotFoundException e) {
            LogUtil.error(log, job.getId(), "sql error:" + ExceptionUtils.getFullStackTrace(e));
            throw new WCoreException("save SQLException!");
        }
    }

    /**
     * get conf from map
     */
    private static <T> T getFromMapByDefault(Map<String,T> maps, String driverKey) {
        return maps.containsKey(driverKey) ? maps.get(driverKey) : maps.get(DRIVER_DEFAULT);
    }

    /**
     * build create table sql
     */
    private static String buildCreateTableSql(String db, String table, StructField[] fields, String driver, int job) {
        String sqlFmt = getFromMapByDefault(sqlFmtMap, driver);
        StringBuffer columnsStmt = new StringBuffer();
        boolean first = true;
        Map<String,String> cMap = getFromMapByDefault(columnMap,driver);
        for(StructField filed: fields) {
            if(!first) {
                columnsStmt.append(", ");
            }
            String type = cMap.get(getTypeName(filed));
            if(type == null) {
                type = cMap.get("default");
            }
            columnsStmt.append("`" + filed.name() + "` " + type);
            first = false;
        }
        String sql = String.format(sqlFmt, db, table, columnsStmt);
        LogUtil.info(log, job, "create sql: " + sql);
        return sql;
    }

    // save type
    private static String getTypeName(StructField filed) {
        String typeName = filed.dataType().typeName().toLowerCase();
        typeName = typeName.replaceAll("\\(.*\\)", "");
        return typeName;
    }

    private Dataset<Row> repartition(Dataset<Row> dataframe, long lines, int partitionNum, int partitionSize, long jobId) {
        if(lines < 1) return dataframe.repartition(1);
        int partition = (int) (lines / partitionSize + 1);
        if(partitionNum > 0) partition = partitionNum;
        LogUtil.info(log, jobId, "data[" + lines + "] repatition to " + partition);
        return dataframe.repartition(partition);
    }

    /**
     * insert data into mysql
     *
     * @throws WCoreException
     */
    private void saveToMysql(JSONObject json, String driver, String table, Dataset<Row> dataframe, List<String> schemas, long job)
            throws WCoreException {
        LogUtil.info(log, job, "start save to mysql...");
        long start = System.currentTimeMillis();
        int batchSize = getFromMapByDefault(batchSizeMap, driver);
        LogUtil.info(log, job, "batch size : "+ batchSize);
        Dataset<Row> ret = dataframe.mapPartitions((Iterator<Row> rows) -> {
            long begin = System.currentTimeMillis();
            LogUtil.info(log, job, "start partition call...");
            List<Row> list = new ArrayList<Row>();
            Connection conn = null;
            try {
                Class.forName(driver);
                String url = (String)json.get("url");
                LogUtil.info(log, job, "insert connect url : "+ url);
                conn = DriverManager.getConnection(url, (String) json.get("user"),(String) json.get("pwd"));
                String[] asks = new String[schemas.size()];
                for (int i = 0; i < schemas.size(); i++) asks[i] = "?";
                String sql = "INSERT INTO " + table + " (" + Joiner.on(",").join(schemas) + ") VALUES(" + Joiner.on(",").join(asks) + ")";
                LogUtil.info(log, job, "insert sql: " + sql);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                int rowCount = 0;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    list.add(row);
                    for (StructField filed : row.schema().fields()) {
                        String name = filed.name();
                        int index = row.fieldIndex(name);
                        switch (getTypeName(filed)) {
                            case "integer":
                                try {
                                    pstmt.setInt(index + 1, row.getInt(index));
                                } catch (Exception e) {
                                    pstmt.setInt(index + 1, 0);
                                }
                                break;
                            case "long":
                                try {
                                    pstmt.setLong(index + 1, row.getLong(index));
                                } catch (Exception e) {
                                    pstmt.setLong(index + 1, 0);
                                }
                                break;
                            case "decimal":
                                try {
                                    pstmt.setDouble(index + 1, row.getDecimal(index).doubleValue());
                                } catch (Exception e) {
                                    pstmt.setDouble(index + 1, 0);
                                }
                                break;
                            case "double":
                                try {
                                    pstmt.setDouble(index + 1, row.getDouble(index));
                                } catch (Exception e) {
                                    pstmt.setDouble(index + 1, 0);
                                }
                                break;
                            default:
                                try {
                                    pstmt.setString(index + 1, row.getString(index));
                                } catch (Exception e) {
                                    pstmt.setString(index + 1, "");
                                }
                                break;
                        }
                    }
                    pstmt.addBatch();
                    rowCount += 1;
                    if (rowCount % batchSize == 0) {
                        pstmt.executeBatch();
                        rowCount = 0;
                    }
                }
                if (rowCount > 0) pstmt.executeBatch();
            } catch (SQLException | ClassNotFoundException e) {
                LogUtil.error(log, job, "sql error:" + ExceptionUtils.getFullStackTrace(e));
                throw new WCoreException("save SQLException: " + e.getMessage());
            } finally {
                if(conn != null) conn.close();
            }
            long end = System.currentTimeMillis();
            log.info("Save to mysql cost: " + (end - begin));
            return list.iterator();
        }, dataframe.org$apache$spark$sql$Dataset$$encoder);
        // trigger action
        long count = ret.count();
        long end = System.currentTimeMillis();
        LogUtil.info(log, job, "save " + count + " lines to mysql cost: " + (end - start) / 1000);
    }
}

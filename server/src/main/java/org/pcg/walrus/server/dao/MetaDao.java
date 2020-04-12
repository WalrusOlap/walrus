package org.pcg.walrus.server.dao;

import com.googlecode.protobuf.format.JsonFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.common.util.EncryptionUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.MetaUtils;
import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.meta.pb.WViewMessage;
import org.pcg.walrus.meta.tree.MTableNode;
import org.pcg.walrus.server.execution.MetaClient;
import org.pcg.walrus.server.model.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * meta client: for meta CURD
 */
@Component
public class MetaDao {

    private static final Logger log = LoggerFactory.getLogger(MetaDao.class);

    @Autowired
    private MetaClient metaClient;

    /**
     * update meta
     */
    private void updateMeta(String oper, String type, String name, String partition) throws Exception {
        // set update info
        String updatePath = String.format("%s/%s_update_%s_%s_%s", MetaConstants.ZK_PATH_UPDATE,
                System.currentTimeMillis(), type, name, partition);
        JSONObject update = new JSONObject();
        update.put("uTime", System.currentTimeMillis());
        update.put("oper", oper);
        update.put("type", type);
        update.put("name", name);
        update.put("partition", partition);
        metaClient.getZkClient().setData(updatePath, update.toJSONString().getBytes());
    }

    /**
     * flush meta
     */
    public void flushMeta() throws Exception {
        // set update info
        String updatePath = String.format("%s/%s_flush", MetaConstants.ZK_PATH_UPDATE, System.currentTimeMillis());
        JSONObject update = new JSONObject();
        update.put("uTime", System.currentTimeMillis());
        update.put("oper", "flush");
        metaClient.getZkClient().setData(updatePath, update.toJSONString().getBytes());
    }

    /**
     * save table to zk
     * @param table
     * @throws Exception
     */
    public void createOrUpdateTable(Table table, String type) throws Exception {
        WTableMessage.WTable.Builder builder = WTableMessage.WTable.getDefaultInstance().toBuilder();
        builder.setTableName(table.getTableName());
        builder.setIsValid(MetaConstants.META_STATUS_VALID);
        builder.setBusiness(table.getBusiness());
        builder.setDateColumn(table.getDateColumn());
        builder.setDesc(table.getDesc());
        builder.setGroup(table.getGroup());
        builder.setStartTime(TimeUtil.DateToString(table.getStartTime(), "yyyy-MM-dd HH:mm:ss"));
        builder.setEndTime(TimeUtil.DateToString(table.getEndTime(), "yyyy-MM-dd HH:mm:ss"));
        builder.setPartitionMode(table.getPartitionMode());
        builder.addAllLevel(table.getLevel());
        String json = EncryptionUtil.toAsciiString(JsonFormat.printToString(builder.build()));
        log.info("create or updte table: "+ json);
        metaClient.getZkClient().setData(tablePath(type, table.getTableName()), json.getBytes());
        updateMeta("add", type, table.getTableName(), "all");
    }

    /**
     * read table from zk
     * @param type
     * @param tableName
     * @return
     */
    public Table descTable(String type, String tableName) throws Exception {
        String zkPath = tablePath(type, tableName);
        if(!metaClient.getZkClient().exists(zkPath)) throw new WServerException(String.format("表不存在: [%s]%s", type, tableName));

        String data = metaClient.getZkClient().getString(zkPath);
        WTableMessage.WTable.Builder builder = WTableMessage.WTable.newBuilder();
        JsonFormat.merge(data, builder);
        WTableMessage.WTable table = builder.build();
        if(table.getIsValid() == MetaConstants.META_STATUS_INVALID)
            throw new WServerException(String.format("table is disable: %s", tableName));
        // generate table
        Table t = new Table(type, tableName);
        t.setGroup(table.getGroup());
        t.setDesc(table.getDesc());
        t.setBusiness(table.getBusiness());
        t.setLevel(table.getLevelList());
        t.setStartTime(TimeUtil.stringToDate(table.getStartTime(),"yyyy-MM-dd HH:mm:ss"));
        t.setEndTime(TimeUtil.stringToDate(table.getEndTime(),"yyyy-MM-dd HH:mm:ss"));
        t.setPartitionMode(table.getPartitionMode());
        t.setPartitions(metaClient.getZkClient().getChildren(zkPath));
        return t;
    }

    /**
     * set table is_valid = 0
     * @param type
     * @param tableName
     * @throws Exception
     */
    public void deleteTable(String type, String tableName) throws Exception {
        String zkPath = tablePath(type, tableName);
        String data = metaClient.getZkClient().getString(zkPath);
        WTableMessage.WTable.Builder builder = WTableMessage.WTable.newBuilder();
        JsonFormat.merge(data, builder);
        builder.setIsValid(MetaConstants.META_STATUS_INVALID);
        String json = JsonFormat.printToString(builder.build());
        metaClient.getZkClient().setData(zkPath, EncryptionUtil.toAsciiString(json).getBytes());
        updateMeta("add", type, tableName, "all");
    }

    // build {WFieldMessage.WUdf}
    private WFieldMessage.WUdf parseUdf(UDF udf) {
        if(udf == null) return null;
        WFieldMessage.WUdf.Builder u = WFieldMessage.WUdf.getDefaultInstance().toBuilder();
        u.setClassName(udf.getClassName());
        u.setMethodName(udf.getMethodName());
        u.addAllParams(udf.getParams());
        return u.build();
    }

    /**
     * load partition
     * @param partition
     * @throws Exception
     */
    public void loadPartition(Partition partition) throws WServerException {
        // lock when load partition
        String partitionPath = partitionPath(partition.getTableType(), partition.getTable(), partition.getPartitionKey());
        String lockPath = lockPath(partition.getTableType(), partition.getTable(), partition.getPartitionKey());
        metaClient.getZkClient().lock(lockPath);
        try {
            // set partition data
            WTableMessage.WPartitionTable.Builder b1 = WTableMessage.WPartitionTable.getDefaultInstance().toBuilder();
            b1.setIsValid(MetaConstants.META_STATUS_VALID);
            b1.setPartitionKey(partition.getPartitionKey());
            for(Field field: partition.getDimensions()) {
                WFieldMessage.WDimension.Builder d = WFieldMessage.WDimension.getDefaultInstance().toBuilder();
                d.setName(field.getName());
                d.setType(field.getType());
                d.setGroup(field.getGroup());
                d.setChinese(field.getChinese());
                d.setDerivedMode(field.getDerivedMode());
                WFieldMessage.WUdf udf = parseUdf(field.getDerivedLogic());
                if(udf != null)
                    d.setDerivedLogic(udf);
                b1.addDimensions(d);
            }
            for(Field field: partition.getMetrics()) {
                WFieldMessage.WMetric.Builder v = WFieldMessage.WMetric.getDefaultInstance().toBuilder();
                v.setName(field.getName());
                v.setType(field.getType());
                v.setMethod(field.getMethod());
                v.setGroup(field.getGroup());
                v.setChinese(field.getChinese());
                v.setDerivedMode(field.getDerivedMode());
                WFieldMessage.WUdf udf = parseUdf(field.getDerivedLogic());
                if(udf != null)
                    v.setDerivedLogic(udf);
                b1.addMetrics(v);
            }
            String j1 = EncryptionUtil.toAsciiString(JsonFormat.printToString(b1.build()));
            log.info("set partition data: " + j1);
            metaClient.getZkClient().setData(partitionPath, j1.getBytes());
            // set base data
            WTableMessage.WBaseTable.Builder b2 = WTableMessage.WBaseTable.getDefaultInstance().toBuilder();
            b2.setBaseName(MetaConstants.META_BASE_TABLE_NAME);
            b2.setStartTime(TimeUtil.DateToString(partition.getStartTime(), "yyyy-MM-dd HH:mm:ss"));
            b2.setEndTime(TimeUtil.DateToString(partition.getEndTime(), "yyyy-MM-dd HH:mm:ss"));
            b2.setIsValid(MetaConstants.META_STATUS_VALID);
            b2.setValidDay(partition.getValidDay());
            String tablet = String.format("tablet_%s", System.currentTimeMillis());
            b2.setCurrentTablet(tablet);
            String basePath = partitionBasePath(partition.getTableType(), partition.getTable(), partition.getPartitionKey());
            String j2 = EncryptionUtil.toAsciiString(JsonFormat.printToString(b2.build()));
            log.info("set partition base data: " + j2);
            metaClient.getZkClient().setData(basePath, j2.getBytes());
            // set tablet data
            WTableMessage.WTablet.Builder b3 = WTableMessage.WTablet.getDefaultInstance().toBuilder();
            b3.setPath(partition.getPath());
            b3.setFormat(partition.getFormat());
            b3.setIsValid(MetaConstants.META_STATUS_VALID);
            b3.setDelim(partition.getDelim());
            b3.setSchema(partition.getSchema());
            b3.setIsBc(partition.getIsBc());
            b3.setBcLogic(partition.getBcLogic());
            b3.setRecordNum(partition.getRecordNum());
            b3.setFileNum(partition.getFileNum());
            b3.setCreateTimestamp(System.currentTimeMillis() + "");
            String tabletPath = String.format("%s/%s", basePath, tablet);
            String j3 = EncryptionUtil.toAsciiString(JsonFormat.printToString(b3.build()));
            log.info("set partition teblet data: " + j3);
            metaClient.getZkClient().setData(tabletPath, j3.getBytes());
            // update table end time
            String tablePath = tablePath(partition.getTableType(), partition.getTable());
            String data = metaClient.getZkClient().getString(tablePath);
            WTableMessage.WTable.Builder builder = WTableMessage.WTable.newBuilder();
            JsonFormat.merge(data, builder);
            Date eday = partition.getEndTime();
            if(eday.after(TimeUtil.stringToDate(builder.getEndTime())))
                builder.setEndTime(TimeUtil.DateToString(eday, "yyyy-MM-dd HH:mm:ss"));
            String tableData = EncryptionUtil.toAsciiString(JsonFormat.printToString(builder.build()));
            metaClient.getZkClient().setData(tablePath, tableData.getBytes());
            // update meta
            updateMeta("update", partition.getTableType(), partition.getTable(), partition.getPartitionKey());
        } catch (Exception e) {
            throw new WServerException(e.getMessage());
        } finally {
            // unlock
            metaClient.getZkClient().unlock(lockPath);
        }
    }

    // build {UDF}
    private UDF parseUdf(WFieldMessage.WUdf udf) {
        UDF u = new UDF();
        u.setClassName(udf.getClassName());
        u.setMethodName(udf.getMethodName());
        u.setParams(udf.getParamsList());
        return u;
    }

    /**
     * load partition from zk
     * @param tableType
     * @param tableName
     * @param partitionKey
     * @return
     */
    public Partition descPartition(String tableType, String tableName, String partitionKey) throws Exception {
        Partition partition = new Partition(tableType, tableName, partitionKey);
        String partitionPath = partitionPath(tableType, tableName, partitionKey);
        if(!metaClient.getZkClient().exists(partitionPath)) throw new WServerException(String.format("分区不存在: [%s]%s.%s",
                partition.getTableType(), partition.getTable(), partition.getPartitionKey()));

        // load partition data
        WTableMessage.WPartitionTable.Builder b1 = WTableMessage.WPartitionTable.newBuilder();
        JsonFormat.merge(metaClient.getZkClient().getString(partitionPath), b1);
        WTableMessage.WPartitionTable t1 = b1.build();
        // check if valid
        if(b1.getIsValid() == MetaConstants.META_STATUS_INVALID)
            throw new WServerException(String.format("partition is disable: %s[%s]", tableName, partitionKey));

        for(WFieldMessage.WDimension d: t1.getDimensionsList()) {
            Field dim = new Field(d.getName());
            dim.setType(d.getType());
            dim.setGroup(d.getGroup());
            dim.setChinese(d.getChinese());
            dim.setDerivedMode(d.getDerivedMode());
            dim.setDerivedLogic(parseUdf(d.getDerivedLogic()));
            partition.addDimension(dim);
        }
        for(WFieldMessage.WMetric d: t1.getMetricsList()) {
            Field metric = new Field(d.getName());
            metric.setType(d.getType());
            metric.setGroup(d.getGroup());
            metric.setChinese(d.getChinese());
            metric.setDerivedMode(d.getDerivedMode());
            metric.setDerivedLogic(parseUdf(d.getDerivedLogic()));
            metric.setMethod(d.getMethod());
            partition.addDimension(metric);
        }
        // load base data
        String basePath = partitionBasePath(partition.getTableType(), partition.getTable(), partition.getPartitionKey());
        WTableMessage.WBaseTable.Builder b2 = WTableMessage.WBaseTable.getDefaultInstance().toBuilder();
        JsonFormat.merge(metaClient.getZkClient().getString(basePath), b2);
        WTableMessage.WBaseTable t2 = b2.build();
        partition.setStartTime(TimeUtil.stringToDate(t2.getStartTime(),"yyyy-MM-dd HH:mm:ss"));
        partition.setEndTime(TimeUtil.stringToDate(t2.getEndTime(),"yyyy-MM-dd HH:mm:ss"));
        partition.setValidDay(t2.getValidDay());
        // load tablet data
        String tabletPath = String.format("%s/%s", basePath, t2.getCurrentTablet());
        WTableMessage.WTablet.Builder b3 = WTableMessage.WTablet.getDefaultInstance().toBuilder();
        JsonFormat.merge(metaClient.getZkClient().getString(tabletPath), b3);
        WTableMessage.WTablet t3 = b3.build();
        partition.setPath(t3.getPath());
        partition.setFormat(t3.getFormat());
        partition.setDelim(t3.getDelim());
        partition.setIsBc(t3.getIsBc());
        partition.setBcLogic(t3.getBcLogic());
        partition.setRecordNum(t3.getRecordNum());
        partition.setFileNum(t3.getFileNum());
        return partition;
    }

    /**
     * set partition is_valid = 0
     * @param tableType
     * @param tableName
     * @param partitionKey
     * @throws Exception
     */
    public void deletePartition(String tableType, String tableName, String partitionKey) throws Exception {
        String partitionPath = partitionPath(tableType, tableName, partitionKey);
        String data = metaClient.getZkClient().getString(partitionPath);
        WTableMessage.WPartitionTable.Builder builder = WTableMessage.WPartitionTable.newBuilder();
        JsonFormat.merge(data, builder);
        builder.setIsValid(MetaConstants.META_STATUS_INVALID);
        metaClient.getZkClient().setData(partitionPath, JsonFormat.printToString(builder.build()).getBytes());
        // update meta
        updateMeta("update", tableType, tableName, partitionKey);
    }

    /**
     *
     * @param view
     * @throws Exception
     */
    public void createOrUpdateView(View view) throws Exception {
        String viewPath = viewPath(view.getName());
        String logic = EncryptionUtil.toAsciiString(view.getView().toJSONString());
        log.info("set view data: " + logic);
        metaClient.getZkClient().setData(viewPath, logic.getBytes());
        updateMeta("add", "view", view.getName(), "all");
    }

    /**
     * desc view
     * @param viewName
     * @return
     * @throws Exception
     */
    public View descView(String viewName) throws Exception {
        String viewPath = viewPath(viewName);
        if(!metaClient.getZkClient().exists(viewPath)) throw new WServerException(String.format("视图不存在: %s", viewName));
        String data = metaClient.getZkClient().getString(viewPath);
        WViewMessage.WView.Builder builder = WViewMessage.WView.newBuilder();
        JsonFormat.merge(data, builder);
        WViewMessage.WView v = builder.build();
        if(v.getIsValid() == MetaConstants.META_STATUS_INVALID)
            throw new WServerException(String.format("view is disable: %s", viewName));
        View view = new View(viewName);
        JSONObject logic = (JSONObject) new JSONParser().parse(JsonFormat.printToString(builder.build()));
        view.setView(logic);
        return view;
    }

    /**
     *
     * @param viewName
     * @throws Exception
     */
    public void deleteView(String viewName) throws Exception {
        String viewPath = viewPath(viewName);
        JSONObject json = (JSONObject) new JSONParser().parse(metaClient.getZkClient().getString(viewPath));
        json.put("isValid", MetaConstants.META_STATUS_INVALID);
        metaClient.getZkClient().setData(viewPath, json.toJSONString().getBytes());
        updateMeta("add", "view", viewName, "all");
    }

    // table path
    private String lockPath(String type, String table, String partition) {
        return String.format("%s/%s/%s/%s", MetaConstants.ZK_PATH_LOCK, type, table, partition);
    }

    // table path
    private String viewPath(String view) {
        return String.format("%s/view/%s", MetaConstants.ZK_PATH_META, view);
    }

    // table path
    private String tablePath(String type, String table) {
        return String.format("%s/%s/%s", MetaConstants.ZK_PATH_META, type, table);
    }

    // partition path
    private String partitionPath(String type, String table, String partition) {
        return String.format("%s/%s/%s/%s", MetaConstants.ZK_PATH_META, type, table, partition);
    }

    // partition path
    private String partitionBasePath(String type, String table, String partition) {
        return String.format("%s/%s/%s/%s/base", MetaConstants.ZK_PATH_META, type, table, partition);
    }

    // WDimension -> Field
    public List<Field> parseDims(Map<String, WFieldMessage.WDimension> dims) {
        List<Field> fields = new ArrayList<>();
        for (WFieldMessage.WDimension dim : dims.values()) {
            Field d = new Field(dim.getName());
            d.setType(dim.getType());
            d.setChinese(dim.getChinese());
            d.setGroup(dim.getGroup());
            d.setDerivedMode(dim.getDerivedMode());
            d.setDerivedLogic(parseUdf(dim.getDerivedLogic()));
            fields.add(d);
        }
        return fields;
    }

    // WMetric -> Field
    public List<Field> parseMetrics(Map<String, WFieldMessage.WMetric> metrics) {
        List<Field> fields = new ArrayList<>();
        for (WFieldMessage.WMetric metric : metrics.values()) {
            Field d = new Field(metric.getName());
            d.setType(metric.getType());
            d.setChinese(metric.getChinese());
            d.setGroup(metric.getGroup());
            d.setMethod(metric.getMethod());
            d.setDerivedMode(metric.getDerivedMode());
            d.setDerivedLogic(parseUdf(metric.getDerivedLogic()));
            fields.add(d);
        }
        return fields;
    }

    // table all columns
    public Map<String, List<Field>> getTableColumns(String tableType, String tableName, Date bday, Date eday, String level) throws Exception {
        MTableNode table = null;
        switch (tableType.toLowerCase()) {
            case MetaConstants.META_TABLE_TYPE_T:
                table = metaClient.getClient().getTables().get(tableName);
                break;
            case MetaConstants.META_TABLE_TYPE_D:
                table = metaClient.getClient().getDicts().get(tableName);
                break;
            default:
                break;
        }
        if(table == null) throw new WServerException(String.format("%s %s not found!", tableType, tableName));
        Map<String, List<Field>> columns = new HashMap<>();
        columns.put(MetaConstants.META_FIELD_DIM, parseDims(table.dims(bday, eday, level, false)));
        columns.put(MetaConstants.META_FIELD_METRIC, parseMetrics(table.metrics(bday, eday, level, false)));
        return columns;
    }

    // view columns
    public Map<String, List<Field>> getViewColumns(String viewName, Date bday, Date eday, String level) throws Exception {
        if(!metaClient.getClient().getViews().containsKey(viewName))
            throw new WServerException(String.format("view %s not found", viewName));
        Map<String, List<Field>> columns = new HashMap<>();
        // dims
        Map<String, WFieldMessage.WDimension> dims = MetaUtils.getViewDims(metaClient.getClient().getViews().get(viewName).getView(),
                metaClient.getClient().getTables(), metaClient.getClient().getDicts(), bday, eday, level);
        columns.put(MetaConstants.META_FIELD_DIM, parseDims(dims));
        // metrics
        Map<String, WFieldMessage.WMetric> metrics = MetaUtils.getViewMetrics(metaClient.getClient().getViews().get(viewName).getView(),
                metaClient.getClient().getTables(), metaClient.getClient().getDicts(), bday, eday, level);
        columns.put(MetaConstants.META_FIELD_METRIC, parseMetrics(metrics));
        return columns;
    }
}

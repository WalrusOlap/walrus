package org.pcg.walrus.server.model;

import com.google.common.base.Joiner;
import org.pcg.walrus.core.CoreConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Date;

/**
 * table partition
 */
public class Partition {

    private String tableType;
    private String table;

    private String partitionKey;
    private int isValid = 1;
    private Date startTime;
    private Date endTime;
    private List<Field> metrics;
    private List<Field> dimensions;
    private String currentTablet;
    private String validDay = "all"; // 按月分区有效日期数据, 例如："1101111100000000000000000"
    private String path; // 文件存储路径
    private String format; //文件存储类型, parquet, csv, hive, clickhouse, mysql...
    private String delim = "\t"; // 文件分隔符
    private int isBc; // 是否广播（针对维度表的map join）. 0:否, 1:是, 2: lazy_bc, 计算时bc
    private String bcLogic; // broadcast logic, sql...
    private long recordNum; // 文件行数
    private long fileNum; // 文件数量

    public Partition(String tableType, String table, String pKey) {
        setTableType(tableType);
        setTable(table);
        setPartitionKey(pKey);
        dimensions = new ArrayList<>();
        metrics = new ArrayList<>();
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public int getIsValid() {
        return isValid;
    }

    public void setIsValid(int isValid) {
        this.isValid = isValid;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public List<Field> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<Field> metrics) {
        this.metrics = metrics;
    }

    public List<Field> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<Field> dimensions) {
        this.dimensions = dimensions;
    }

    public void addDimension(Field dim) {
        dimensions.add(dim);
    }

    public void addMetric(Field metric) {
        metrics.add(metric);
    }

    public String getCurrentTablet() {
        return currentTablet;
    }

    public void setCurrentTablet(String currentTablet) {
        this.currentTablet = currentTablet;
    }

    public String getValidDay() {
        return validDay;
    }

    public void setValidDay(String validDay) {
        this.validDay = validDay;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * @return dim_1:dim_type,dim_2:dim_type..metric_1:metric_type...
     */
    public String getSchema() {
        List<String> fields = new ArrayList<>();
        if(dimensions != null)
            for(Field dim: dimensions)
                if(CoreConstants.DERIVE_MODE_SELECT.equals(dim.getDerivedMode()))
                    fields.add(String.format("%s:%s", dim.getName(), dim.getType()));
        if(metrics != null)
            for(Field metric: metrics)
                if(CoreConstants.DERIVE_MODE_SELECT.equals(metric.getDerivedMode()))
                    fields.add(String.format("%s:%s", metric.getName(), metric.getType()));
        return Joiner.on(",").join(fields);
    }

    public String getDelim() {
        return delim;
    }

    public void setDelim(String delim) {
        this.delim = delim;
    }

    public int getIsBc() {
        return isBc;
    }

    public void setIsBc(int isBc) {
        this.isBc = isBc;
    }

    public String getBcLogic() {
        return bcLogic;
    }

    public void setBcLogic(String bcLogic) {
        this.bcLogic = bcLogic;
    }

    public long getRecordNum() {
        return recordNum;
    }

    public void setRecordNum(long recordNum) {
        this.recordNum = recordNum;
    }

    public long getFileNum() {
        return fileNum;
    }

    public void setFileNum(long fileNum) {
        this.fileNum = fileNum;
    }

    @Override
    public String toString() {
        return tableType + "/" + table + ":" + partitionKey;
    }
}

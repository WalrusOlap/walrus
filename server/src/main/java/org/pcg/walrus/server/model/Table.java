package org.pcg.walrus.server.model;

import java.util.List;
import java.util.Date;

/**
 * walrus table
 * 	optional string tableName = 1; // 表名
 * 	optional string group = 2; // 分组
 * 	optional string business = 3[default = "unknown"]; // 业务
 * 	optional string desc = 4; //描述
 * 	optional string startTime = 5; //提供查询的有效起始时间
 * 	optional string endTime = 6; //提供查询的有效结束时间
 * 	optional string partitionMode = 7;  // 分区类型, A:全量, Y:按年分表, M:按月分表, D:按天分表, H:按小时分表
 * 	optional string partitionLogic = 8;  // 分区逻辑
 * 	repeated string cluster = 9;// 计算集群, 可多选 online, offline...
 * 	optional string dateColumn = 10[default = "date"];// 日期字段，过滤时间范围用
 */
public class Table {

    private String type; // table | dict
    private String tableName;
    private String desc;
    private String group; // table group
    private String business; // table business
    private String partitionMode;
    private List<String> level; // online | offline
    private Date startTime;
    private Date endTime;
    private String dateColumn = "date";
    private List<String> partitions;

    // constructor
    public Table(String type, String tableName) {
        setTableName(tableName);
        setType(type);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }

    public String getPartitionMode() {
        return partitionMode;
    }

    public void setPartitionMode(String partitionMode) {
        this.partitionMode = partitionMode;
    }

    public List<String> getLevel() {
        return level;
    }

    public void setLevel(List<String> level) {
        this.level = level;
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

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public String getDateColumn() {
        return dateColumn;
    }

    public void setDateColumn(String dateColumn) {
        this.dateColumn = dateColumn;
    }

    @Override
    public String toString() {
        return tableName + "(" + type + "," + partitionMode + "):" + desc;
    }
}

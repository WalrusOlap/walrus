package org.pcg.walrus.server.service;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.server.dao.MetaDao;
import org.pcg.walrus.server.model.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class MetaService {

    private static final Logger log = LoggerFactory.getLogger(MetaService.class);

    @Autowired
    private MetaDao metaDao;

    /**
     * flush meta
     */
    public void flushMeta() throws Exception {
        metaDao.flushMeta();
    }

    // ==============
    // table
    // ==============
    /**
     * create or update table
     */
    public void createOrUpdateTable(String type, String name, String partitionMode, String levels, Date startTime,
            Date endTime, String desc, String business, String group) throws Exception {
        Table table = new Table(type, name);
        table.setPartitionMode(partitionMode);
        table.setBusiness(business);
        List<String> levelist = new ArrayList<>();
        for(String level: levels.split(",")) levelist.add(level.toLowerCase());
        // level offline is necessary
        if(!levelist.contains(MetaConstants.CLUSTER_LEVEL_OFFLINE))
            throw new WServerException(String.format("level %s is necessary!", MetaConstants.CLUSTER_LEVEL_OFFLINE));
        table.setLevel(levelist);
        table.setStartTime(startTime);
        table.setEndTime(endTime);
        table.setDesc(desc);
        table.setGroup(group);
        // save
        metaDao.createOrUpdateTable(table, type);
    }

    /**
     * @param table
     */
    public Table descTable(String type, String table) throws Exception {
        return metaDao.descTable(type, table);
    }

    /**
     * @param table
     */
    public void deleteTable(String type, String table) throws Exception {
        metaDao.deleteTable(type, table);
    }

    // ==============
    // partition
    // ==============

    /**
     *
     * 	 {
     * 	     "name": "col_name",
     * 	     "type": "col_type",
     * 	     "group": "col_group",
     * 	     "chinese": "col_chinese",
     * 	     "derivedMode": "col_derived_mode",
     * 	     "derivedLogic": {
     *          "className": "DEFAULT_CLASS",
     *          "methodName": "DEFAULT_METHOD",
     *          "params": ["param_1","param_2"]
     * 	     }
     * 	 }
     */
    private List<Field> parseFields(String fieldStr) throws ParseException {
        List<Field> fields = new ArrayList<>();
        JSONArray array = (JSONArray) new JSONParser().parse(fieldStr);
        for(int i = 0; i<array.size(); i++) {
            JSONObject json = (JSONObject) array.get(i);
            Field field = new Field(json.get("name").toString());
            field.setMethod(json.getOrDefault("method", CoreConstants.METRIC_FUNCTION_SUM).toString());
            field.setType(json.getOrDefault("type", CoreConstants.COLUMN_TYPE_STRING).toString());
            field.setGroup(json.getOrDefault("group", "").toString());
            field.setChinese(json.getOrDefault("chinese", "").toString());
            String derivedMode = json.getOrDefault("derivedMode", CoreConstants.DERIVE_MODE_SELECT).toString();
            field.setDerivedMode(derivedMode);
            if(CoreConstants.DERIVE_MODE_EXTEND.equalsIgnoreCase(derivedMode)
                || CoreConstants.DERIVE_MODE_EXT1.equalsIgnoreCase(derivedMode)) {
                JSONObject derivedLogic = (JSONObject) json.get("derivedLogic");
                UDF udf = new UDF();
                udf.setClassName(derivedLogic.getOrDefault("className",CoreConstants.UDF_CLASS_DEFAULT).toString());
                udf.setMethodName(derivedLogic.getOrDefault("methodName",CoreConstants.UDF_METHOD_DEFAULT).toString());
                udf.setParams((JSONArray) derivedLogic.get("params"));
                field.setDerivedLogic(udf);
            }
            //
            fields.add(field);
        }
        return fields;
    }

    /**
     * load partition
     */
    public void loadPartition(String type, String table, String partitionKey, Date startTime, Date endTime,
             String dimensions, String metrics, String path, String format, String validDay,
             String delim, int isBc, String bcLogic, long recordNum, long fileNum) throws Exception  {
        Partition partition = new Partition(type, table, partitionKey);
        partition.setStartTime(startTime);
        partition.setEndTime(endTime);
        // dimensions
        partition.setDimensions(parseFields(dimensions));
        // metrics
        partition.setMetrics(parseFields(metrics));
        partition.setPath(path);
        partition.setFormat(format);
        partition.setValidDay(validDay);
        partition.setDelim(delim);
        partition.setIsBc(isBc);
        partition.setBcLogic(bcLogic);
        partition.setRecordNum(recordNum);
        partition.setFileNum(fileNum);
        // load to zk
        metaDao.loadPartition(partition);
    }

    /**
     *
     * @param tableType
     * @param tableName
     * @param partition
     * @return
     */
    public Partition descPartition(String tableType, String tableName, String partition) throws Exception {
        return metaDao.descPartition(tableType, tableName, partition);
    }

    /**
     *
     * @param tableType
     * @param tableName
     * @param partition
     */
    public void deletePartition(String tableType, String tableName, String partition) throws Exception {
        metaDao.deletePartition(tableType, tableName, partition);
    }

    // ==============
    // view
    // ==============
    /**
     * @param
     */
    public void createOrUpdateView(String name, String loigc) throws Exception {
        View view = new View(name);
        view.setView((JSONObject) new JSONParser().parse(loigc));
        metaDao.createOrUpdateView(view);
    }

    private void checkView() {

    }

    /**
     * @param view
     */
    public View descView(String view) throws Exception {
        return metaDao.descView(view);
    }

    /**
     * @param view
     */
    public void deleteView(String view) throws Exception {
        metaDao.deleteView(view);
    }

    // ==============
    // fields
    // ==============
    public Map<String, List<Field>> getTableColumns(String tableType, String tableName, Date bday, Date eday, String level)
            throws Exception {
        return metaDao.getTableColumns(tableType, tableName, bday, eday, level);

    }

    public Map<String, List<Field>> getViewColumns(String view, Date bday, Date eday, String level) throws Exception {
        return metaDao.getViewColumns(view, bday, eday, level);
    }

}

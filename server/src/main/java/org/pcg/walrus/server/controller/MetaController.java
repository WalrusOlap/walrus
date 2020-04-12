package org.pcg.walrus.server.controller;

import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.server.model.*;
import org.pcg.walrus.server.service.MetaService;
import org.pcg.walrus.server.model.ResponseTemplate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import org.apache.commons.httpclient.HttpStatus;

import java.util.Date;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value="/meta")
public class MetaController {

    private static final Logger log = LoggerFactory.getLogger(MetaController.class);

    @Autowired
    private MetaService metaService;

    @GetMapping(value="/flush")
    public ResponseTemplate<Object> flushMeta() throws Exception  {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        metaService.flushMeta();
        return response;
    }

    // ==============
    // view
    // ==============
    @PostMapping(value="/view/createOrUpdate")
    public ResponseTemplate<View> createView(@RequestParam(value = "name", required = true) String name,
                                             @RequestParam(value = "view", required = true) String logic) throws Exception  {
        metaService.createOrUpdateView(name, logic);
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        return response;
    }

    @GetMapping(value="/view/desc/{name}")
    public ResponseTemplate<View> getView(@PathVariable String name) throws Exception  {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(metaService.descView(name));
        return response;
    }

    @GetMapping(value="/view/delete/{name}")
    public ResponseTemplate<View> deleteView(@PathVariable String name) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        metaService.deleteView(name);
        return response;
    }

    @PostMapping(value="/view/fields")
    public ResponseTemplate<Map<String, List<Field>>> viewFields(@RequestParam(value = "name", required = true) String view,
            @RequestParam(value = "level", required = false, defaultValue = MetaConstants.CLUSTER_LEVEL_ALL) String level,
            @RequestParam(value = "startTime", required = false, defaultValue = "1997-01-01 00:00:00") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date bday,
            @RequestParam(value = "endTime", required = false, defaultValue = "2050-12-31 23:59:59") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date eday) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(metaService.getViewColumns(view, bday, eday, level));
        return response;
    }

    // ==============
    // table
    // ==============
    @PostMapping(value="/table/createOrUpdate")
    public ResponseTemplate<Table> createTable(@RequestParam(value = "name", required = true) String name,
           @RequestParam(value = "partitionMode", required = true) String partitionMode,
           @RequestParam(value = "levels", required = true) String levels,
           @RequestParam(value = "startTime", required = true) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date startTime,
           @RequestParam(value = "endTime", required = true) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date endTime,
           @RequestParam(value = "desc", required = false, defaultValue = "unknown") String desc,
           @RequestParam(value = "business", required = false, defaultValue = "unknown") String business,
           @RequestParam(value = "group", required = false, defaultValue = "unknown") String group) throws Exception  {
        metaService.createOrUpdateTable(MetaConstants.META_TABLE_TYPE_T, name, partitionMode, levels, startTime, endTime, desc, business, group);
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        return response;
    }

    @GetMapping(value="/table/desc/{name}")
    public ResponseTemplate<Table> descTable(@PathVariable String name) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(metaService.descTable(MetaConstants.META_TABLE_TYPE_T, name));
        return response;
    }

    @GetMapping(value="/table/delete/{name}")
    public ResponseTemplate<Table> deleteTable(@PathVariable String name) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        metaService.deleteTable(MetaConstants.META_TABLE_TYPE_T, name);
        return response;
    }

    @PostMapping(value="/table/fields")
    public ResponseTemplate<Map<String, List<Field>>> tableFields(@RequestParam(value = "name", required = true) String table,
         @RequestParam(value = "level", required = false, defaultValue = MetaConstants.CLUSTER_LEVEL_ALL) String level,
         @RequestParam(value = "startTime", required = false, defaultValue = "1997-01-01 00:00:00") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date bday,
         @RequestParam(value = "endTime", required = false, defaultValue = "2050-12-31 23:59:59") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date eday) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(metaService.getTableColumns(MetaConstants.META_TABLE_TYPE_T, table, bday, eday, level));
        return response;
    }

    // ==============
    // dict
    // ==============
    @PostMapping(value="/dict/createOrUpdate")
    public ResponseTemplate<Table> createDict(@RequestParam(value = "name", required = true) String name,
           @RequestParam(value = "partitionMode", required = true) String partitionMode,
           @RequestParam(value = "levels", required = true) String levels,
           @RequestParam(value = "startTime", required = true) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date startTime,
           @RequestParam(value = "endTime", required = true) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date endTime,
           @RequestParam(value = "desc", required = false, defaultValue = "unknown") String desc,
           @RequestParam(value = "business", required = false, defaultValue = "unknown") String business,
           @RequestParam(value = "group", required = false, defaultValue = "unknown") String group) throws Exception  {
        metaService.createOrUpdateTable(MetaConstants.META_TABLE_TYPE_D, name, partitionMode, levels, startTime, endTime, desc, business, group);
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        return response;
    }

    @GetMapping(value="/dict/desc/{name}")
    public ResponseTemplate<Table> descDict(@PathVariable String name) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(metaService.descTable(MetaConstants.META_TABLE_TYPE_D, name));
        return response;
    }

    @GetMapping(value="/dict/delete/{name}")
    public ResponseTemplate<Table> deleteDict(@PathVariable String name) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        metaService.deleteTable(MetaConstants.META_TABLE_TYPE_D, name);
        return response;
    }

    @PostMapping(value="/dict/fields")
    public ResponseTemplate<Map<String, List<Field>>> dictFields(@RequestParam(value = "name", required = true) String dict,
          @RequestParam(value = "level", required = false, defaultValue = MetaConstants.CLUSTER_LEVEL_ALL) String level,
          @RequestParam(value = "startTime", required = false, defaultValue = "1997-01-01 00:00:00") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date bday,
          @RequestParam(value = "endTime", required = false, defaultValue = "2050-12-31 23:59:59") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date eday) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(metaService.getTableColumns(MetaConstants.META_TABLE_TYPE_D, dict, bday, eday, level));
        return response;
    }

    // ==============
    // partition
    // ==============
    @PostMapping(value="/partition/load")
    public ResponseTemplate<Partition> loadPartition(@RequestParam(value = "tableType", required = true) String tableType,
         @RequestParam(value = "tableName", required = true) String tableName,
         @RequestParam(value = "partitionKey", required = true) String partitionKey,
         @RequestParam(value = "startTime", required = true) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date startTime,
         @RequestParam(value = "endTime", required = true) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date endTime,
         @RequestParam(value = "dimensions", required = true) String dimensions,
         @RequestParam(value = "metrics", required = true) String metrics,
         @RequestParam(value = "path", required = true) String path,
         @RequestParam(value = "format", required = true) String format,
         @RequestParam(value = "validDay", required = false, defaultValue = "all") String validDay,
         @RequestParam(value = "delim", required = false, defaultValue = "\t") String delim,
         @RequestParam(value = "isBc", required = false, defaultValue = "0") int isBc,
         @RequestParam(value = "bcLogic", required = false, defaultValue = "") String bcLogic,
         @RequestParam(value = "recordNum", required = false, defaultValue = "0") long recordNum,
         @RequestParam(value = "fileNum", required = false, defaultValue = "0") long fileNum) throws Exception  {
        metaService.loadPartition(tableType, tableName, partitionKey, startTime, endTime,
                dimensions, metrics, path, format, validDay,
                delim, isBc, bcLogic, recordNum, fileNum);
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        return response;
    }

    @GetMapping(value="/partition/desc/{tableType}/{tableName}/{partitionKey}")
    public ResponseTemplate<Partition> descPartition(@PathVariable(value = "tableType", required = true) String tableType,
            @PathVariable(value = "tableName", required = true) String tableName,
            @RequestParam(value = "partitionKey", required = true) String partitionKey) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(metaService.descPartition(tableType, tableName, partitionKey));
        return response;
    }


    @GetMapping(value="/partition/delete/{tableType}/{tableName}/{partitionKey}")
    public ResponseTemplate<Partition> deletePartition(@PathVariable(value = "tableType", required = true) String tableType,
           @PathVariable(value = "tableName", required = true) String tableName,
           @RequestParam(value = "partitionKey", required = true) String partitionKey) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        metaService.deletePartition(tableType, tableName, partitionKey);
        return response;
    }

}


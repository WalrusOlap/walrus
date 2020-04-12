package org.pcg.walrus.examples;

import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

import java.io.File;

public class MetaClient extends WalrusClient {

    public void createTable(String table) throws Exception {
        JSONObject json = readJson(String.format("/meta/table/%s", table));
        String url = "meta/table/createOrUpdate";
        createOrUpdate(url, json);
    }

    public void createDict(String dict)throws Exception {
        JSONObject json = readJson(String.format("/meta/dict/%s", dict));
        String url = "meta/dict/createOrUpdate";
        createOrUpdate(url, json);
    }

    private void createOrUpdate(String url, JSONObject json) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", json.getOrDefault("tableName", "").toString());
        params.put("partitionMode", json.getOrDefault("partitionMode", "").toString());
        params.put("levels", json.getOrDefault("levels", "").toString());
        params.put("group", json.getOrDefault("group", "").toString());
        params.put("business", json.getOrDefault("business", "").toString());
        params.put("desc", json.getOrDefault("desc", "").toString());
        params.put("startTime", json.getOrDefault("startTime","").toString());
        params.put("endTime", json.getOrDefault("endTime","offline").toString());
        System.out.println(post(url, params));
    }

    public void createView(String view) throws Exception {
        JSONObject json = readJson(String.format("/meta/view/%s", view));
        String url = "meta/view/createOrUpdate";
        Map<String, String> params = new HashMap<>();
        params.put("name", json.get("name").toString());
        params.put("view", json.toJSONString());
        System.out.println(post(url, params));
    }

    public void loadPartition(String partition) throws Exception {
        JSONObject json = readJson(String.format("/meta/partition/%s", partition));
        Map<String, String> params = new HashMap<>();
        params.put("tableType", json.getOrDefault("tableType", "").toString());
        params.put("tableName", json.getOrDefault("tableName", "").toString());
        params.put("partitionKey", json.getOrDefault("partitionKey", "").toString());
        params.put("startTime", json.getOrDefault("startTime","").toString());
        params.put("endTime", json.getOrDefault("endTime","offline").toString());
        JSONAware dimensions = (JSONAware) json.get("dimensions");
        params.put("dimensions", dimensions.toJSONString());
        JSONAware metrics = (JSONAware) json.get("metrics");
        params.put("metrics", metrics.toJSONString());
        params.put("path", json.getOrDefault("path", "").toString());
        params.put("format", json.getOrDefault("format", "").toString());
        params.put("validDay", json.getOrDefault("validDay", "").toString());
        params.put("delim", json.getOrDefault("delim", "").toString());
        params.put("isBc", json.getOrDefault("isBc", "").toString());
        params.put("bcLogic", json.getOrDefault("bcLogic", "").toString());
        params.put("recordNum", json.getOrDefault("recordNum", "").toString());
        params.put("fileNum", json.getOrDefault("fileNum", "").toString());
        String url = "/meta/partition/load";
        System.out.println(post(url, params));
    }

    public void descTable(String table) throws Exception {
        String url = String.format("/meta/table/desc/%s", table);
        System.out.println(get(url));
    }

    public void descDict(String dict) throws Exception {
        String url = String.format("/meta/dict/desc/%s", dict);
        System.out.println(get(url));
    }

    public void descView(String view) throws Exception {
        String url = String.format("/meta/view/desc/%s", view);
        System.out.println(get(url));
    }

    public void deleteTable(String table) throws Exception {
        String url = String.format("/meta/table/delete/%s", table);
        System.out.println(get(url));
    }

    public void deleteDict(String dict) throws Exception {
        String url = String.format("/meta/dict/delete/%s", dict);
        System.out.println(get(url));
    }

    public void deleteView(String view) throws Exception {
        String url = String.format("/meta/view/delete/%s", view);
        System.out.println(get(url));
    }

    // example entrance
    public static void main(String[] args) throws Exception {
        MetaClient client = new MetaClient();
        // create table
        File tables = new File(MetaClient.class.getResource("/meta/table").toURI());
        for(File table: tables.listFiles()) client.createTable(table.getName());
        // create dict
        File dicts = new File(MetaClient.class.getResource("/meta/dict").toURI());
        for(File dict: dicts.listFiles()) client.createDict(dict.getName());
        // create view
        File views = new File(MetaClient.class.getResource("/meta/view").toURI());
        for(File view: views.listFiles()) client.createView(view.getName());
        // load partition
        File ps = new File(MetaClient.class.getResource("/meta/parition").toURI());
        for(File p: ps.listFiles()) client.loadPartition(p.getName());
    }


}

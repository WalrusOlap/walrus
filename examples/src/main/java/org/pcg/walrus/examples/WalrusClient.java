package org.pcg.walrus.examples;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class WalrusClient {

    private CloseableHttpClient  httpclient = HttpClients.createDefault();

    private static String walrusUrl;

    static {
        Map<String, String> properties = readProperties("/application.properties");
        walrusUrl = properties.get("walrus.url");
    }

    // get walrus
    protected JSONObject get(String request) throws Exception {
        HttpGet httpget = new HttpGet(String.format("%s/%s", walrusUrl, request));
        CloseableHttpResponse response = httpclient.execute(httpget);
        return handleResult(response);
    }

    // post walrus
    protected JSONObject post(String request, Map<String, String> parmas)
            throws Exception {
        HttpPost  httpPost = new HttpPost (String.format("%s/%s", walrusUrl, request));
        List<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>();
        for (String k : parmas.keySet()) {
            parameters.add(new BasicNameValuePair(k, parmas.get(k)));
        }

        httpPost.setEntity(new UrlEncodedFormEntity(parameters, "utf-8"));
        CloseableHttpResponse response = httpclient.execute(httpPost);
        return handleResult(response);
    }

    private JSONObject handleResult(CloseableHttpResponse response) throws Exception {
        JSONObject json = null;
        try {
            if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
                throw new Exception("post error[" + response.getStatusLine().getStatusCode() + "]:" + response.toString());
            HttpEntity entity = response.getEntity();
            String jsonStr = EntityUtils.toString(entity, StandardCharsets.UTF_8);
            json = (JSONObject) new JSONParser().parse(jsonStr);
        } finally {
            response.close();
        }
        return json;
    }

    /**
     * read json
     */
    protected JSONObject readJson(String file) throws Exception {
        InputStream input = WalrusClient.class.getResourceAsStream(file);
        StringBuffer sb = new StringBuffer();
        for(String line: IOUtils.readLines(input)) sb.append(line);
        return (JSONObject) new JSONParser().parse(sb.toString());
    }

    /**
     * read properties file in class path
     */
    public static Map<String, String> readProperties(String file) {
        Map<String, String> properties = new HashMap<>();
        Properties p = new Properties();
        InputStream input = WalrusClient.class.getResourceAsStream(file);
        try {
            p.load(input);
            for (String k : p.stringPropertyNames()) properties.put(k, p.getProperty(k));
        } catch (IOException e) {}
        return properties;
    }
}
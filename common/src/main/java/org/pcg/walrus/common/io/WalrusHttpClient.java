package org.pcg.walrus.common.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *  HTTP client
 */
public class WalrusHttpClient {

    private CloseableHttpClient  httpclient;

    public WalrusHttpClient() {
        httpclient = HttpClients.createDefault();
    }

    /**
     * get
     */
    public void get(String request) throws IOException {
        HttpGet httpget = new HttpGet(request);
        CloseableHttpResponse response = httpclient.execute(httpget);
        try {
            response.getEntity();
        } finally {
            response.close();
        }
    }

    /**
     * post
     */
    public void post(String url, Map<String, String> parmas) throws IOException {
        HttpPost  httpPost = new HttpPost (url);
        List<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>();
        for (String k : parmas.keySet()) {
            String v = parmas.get(k);
            parameters.add(new BasicNameValuePair(k, v));
        }
        httpPost.setEntity(new UrlEncodedFormEntity(parameters, "utf-8"));
        CloseableHttpResponse response = httpclient.execute(httpPost);
        try {
            response.getStatusLine();
//            if(status.getStatusCode() != HttpStatus.SC_OK);
        } finally {
            response.close();
        }
    }

    /**
     * post
     * @throws ParseException
     */
    public JSONAware postForResponse(String url, Map<String, Object> parmas)
            throws IOException, ParseException {
        HttpPost  httpPost = new HttpPost (url);
        List<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>();
        for (String k : parmas.keySet()) {
            Object v = parmas.get(k);
            parameters.add(new BasicNameValuePair(k, v.toString()));
        }
        httpPost.setEntity(new UrlEncodedFormEntity(parameters, "utf-8"));
        CloseableHttpResponse response = httpclient.execute(httpPost);
        JSONAware json = null;
        try {
            if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
                throw new IOException("post error[" + response.getStatusLine().getStatusCode() + "]:" + response.toString());
            HttpEntity entity = response.getEntity();
            String jsonStr = EntityUtils.toString(entity, StandardCharsets.UTF_8);
            json = (JSONAware) new JSONParser().parse(jsonStr);
        } finally {
            response.close();
        }
        return json;
    }
}

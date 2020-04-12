package org.pcg.walrus.core.test;

import org.junit.Test;
import org.pcg.walrus.core.parse.WJsonParser;
import org.pcg.walrus.core.parse.WSqlParser;
import org.pcg.walrus.core.query.RQuery;

import java.util.regex.Pattern;

public class TestParser {

    @Test
    public void testSqlParser() throws Exception {
        String sql = "SELECT 2019, date/100 month, getQuarter(date) quarter, date as t_date, location_id, " +
            // "case when location_id=1 then 'l1' else 'l2' end AS loc_type," +
            "sum(imp),sum(click)/sum(imp) ctr, sum(click) click, 1000*sum(income)/sum(imp) ecpm " +
            // ", avg(click/imp) avg_ctr, sum(click/imp) sum_ctr " +
            "from daily_xinwen_view " +
            "where date  between  20191201 and 20191211 and location_id!=3 and age between 10 and 20 and oid in (1,2,3) " +
            "group by date/100, getQuarter(date), `date` , location_id " +
             // ", location_id,case when location_id=1 then 'l1' else 'l2' end " +
            "ORDER BY ctr desc " +
            "LIMIT 1000";
        WSqlParser parser = new WSqlParser();
        RQuery query = parser.parseQuery(sql);
        System.out.println(query);
    }

    @Test
    public void testJsonParser() throws Exception {
        String json = "{    \n" +
                "    \"table\": \"web\",\n" +
                "    \"metric\": [\n" +
                "      {\"name\": \"imp\", \"method\": \"min\", \"alias\": \"min_imp\"},\n" +
                "      {\"name\": \"click\", \"method\": \"sum\"},\n" +
                "      {\"name\": \"uv\"},\n" +
                "      {\"name\": \"imp\", \"method\": \"max\", \"alias\": \"max_imp\"}\n" +
                "    ],\n" +
                "    \"where\": \"location_id='App_Stream_news_news'\",\n" +
                "    \"group_by\": \"location_id\",\n" +
                "    \"end_day\": \"20191101\",\n" +
                "    \"begin_day\": \"20191101\"\n" +
                "  }";
        WJsonParser parser = new WJsonParser();
        RQuery query = parser.parseQuery(json);
        System.out.println(query);
    }

    @Test
    public void testUv() throws Exception {
        String sql = "SELECT location_id,count(distinct omgid) uv,1000*sum(income)/sum(imp) ecpm from daily_xinwen_view where date_column  between  20191201 and 20191211 group by location_id";
        WSqlParser parser = new WSqlParser();
        RQuery query = parser.parseQuery(sql);
        System.out.println(query);
    }
}

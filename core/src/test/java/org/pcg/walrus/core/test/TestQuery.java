package org.pcg.walrus.core.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.pcg.walrus.core.parse.WJsonParser;
import org.pcg.walrus.core.query.RColumn;
import org.pcg.walrus.core.query.RCondition;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;
import org.junit.Test;

import org.pcg.walrus.common.exception.WCoreException;

public class TestQuery {

	@Test
	public void testAddCondition() throws WCoreException {
		String json = "{\"metric\": \"imp\", \"begin_day\": \"20190101\", \"where\": \"\", \"end_day\": \"20190101\", \"table\": \"xinwen\", \"group_by\": \"year,date\"}";
		RQuery query = new WJsonParser().parseQuery(json);
		RCondition condition = query.getCondition();
		condition.addCondition("date between 20190101 and 20190101");
		assertTrue("1 = 1 AND date BETWEEN 20190101 AND 20190101".equals(condition.getWhere()));
	}

	@Test
	public void testCondition() throws WCoreException {
		String json = "{\"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"where\": \"loc_code='3567' or order_class='9'\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		RQuery query = new WJsonParser().parseQuery(json);
		RCondition condition = query.getCondition();
		RCondition c1 = condition.copy();
		c1.addCondition("is_offline=0");
		assertTrue("loc_code = '3567' OR order_class = '9'".equals(condition.getWhere()));
		assertTrue("(loc_code = '3567' OR order_class = '9') AND is_offline = 0".equals(c1.getWhere()));
		String json2 = "{\"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"where\": \"\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		RQuery query2 = new WJsonParser().parseQuery(json2);
		RCondition condition2 = query2.getCondition();
		RCondition c2 = condition2.copy();
		c2.addCondition("is_offline=0");
		assertTrue("1 = 1".equals(condition2.getWhere()));
		assertTrue("1 = 1 AND is_offline = 0".equals(c2.getWhere()));
	}

	@Test
	public void testField() {
		RColumn[] cols = new RColumn[]{
				new RColumn("col_1"),new RColumn("col_2"),new RColumn("col_3")
		};
		assertTrue(RColumn.addColumn(cols, new RColumn("col_4")).length == 4);
		assertTrue(RColumn.addColumn(cols, new RColumn("col_3")).length == 3);
		assertTrue(RColumn.removeColumn(cols, new RColumn("col_4")).length == 3);
		assertTrue(RColumn.removeColumn(cols, new RColumn("col_3")).length == 2);
		RColumn newC = new RColumn("col_3", "String");
		newC.setExType("join");
		assertTrue(RColumn.addColumn(cols, newC)[2].getDerivedMode().equals("join"));

		RMetric[] ms = new RMetric[]{
				new RMetric("col_1"),new RMetric("col_2"),new RMetric("col_3")
		};
		assertTrue(RMetric.addMetric(ms, new RMetric("col_4")).length == 4);
		assertTrue(RMetric.addMetric(ms, new RMetric("col_3")).length == 3);
		assertTrue(RMetric.removeMetric(ms, new RMetric("col_4")).length == 3);
		assertTrue(RMetric.removeMetric(ms, new RMetric("col_3")).length == 2);
		RMetric newM = new RMetric("col_3", "String");
		newM.setExType("join");
		assertTrue(RMetric.addMetric(ms, newM)[2].getDerivedMode().equals("join"));
	}


	@Test
	public void testMatch() throws WCoreException {
		String json = "{\"where\": \"order_class='CMP' and loid=1\", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		RQuery query = new WJsonParser().parseQuery(json);
		RCondition condition = query.getCondition();
		assertTrue(condition.match("order_class='CMP'"));

		String json2 = "{\"where\": \"order_class='CMP' or loid=1\", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		RQuery query2 = new WJsonParser().parseQuery(json2);
		RCondition condition2 = query2.getCondition();
		assertFalse(condition2.match("order_class='CMP'"));
	}

	@Test
	public void testReplace() throws WCoreException {
		// test and
		String json = "{\"where\": \"( m_cover_id=827397 ) and (is_offline='0'  or  is_live ='0') \", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		RQuery query = new WJsonParser().parseQuery(json);
		RCondition condition = query.getCondition();
		condition.replaceColumn("is_live", "a.is_live");

		assertTrue("m_cover_id = 827397 AND (is_offline = '0' OR a.is_live = '0')".equals(condition.getWhere()));
		// test in
		json = "{\"where\": \"oid in (1000, 5000,  '87666') \", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		query = new WJsonParser().parseQuery(json);
		condition = query.getCondition();
		condition.replaceColumn("oid", "b.oid");
		assertTrue("b.oid IN (1000, 5000, '87666')".equals(condition.getWhere()));
		// test oid vs. loid
		json = "{\"where\": \"oid in (1000, 5000,  '87666') and loid=10006 \", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		query = new WJsonParser().parseQuery(json);
		condition = query.getCondition();
		condition.replaceColumn("oid", "a.oid");
		condition.replaceColumn("loid", "c.loid");
		assertTrue("a.oid IN (1000, 5000, '87666') AND c.loid = 10006".equals(condition.getWhere()));
		// test or
		json = "{\"where\": \"( m_cover_id=827397 ) or (is_offline='0'  and  is_live ='0')\", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		query = new WJsonParser().parseQuery(json);
		condition = query.getCondition();
		condition.replaceColumn("is_offline", "a.is_offline");
		condition.replaceColumn("is_live", "b.is_live");
		assertTrue("m_cover_id = 827397 OR (a.is_offline = '0' AND b.is_live = '0')".equals(condition.getWhere()));
		// test greater then | less than
		json = "{\"where\": \"oid>20000000 and loid<=0 and display_id>=15 and mid<10\", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		query = new WJsonParser().parseQuery(json);
		condition = query.getCondition();
		condition.replaceColumn("oid", "a.oid");
		condition.replaceColumn("loid", "b.loid");
		condition.replaceColumn("display_id", "c.display_id");
		condition.replaceColumn("mid", "d.mid");
		assertTrue("a.oid > 20000000 AND b.loid <= 0 AND c.display_id >= 15 AND d.mid < 10".equals(condition.getWhere()));

		// test not
		json = "{\"where\": \"(location_type != 'ld' AND NOT (exe_source = 'gp' AND res_site = 'APP' AND location_type NOT LIKE '%yd%'))\", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		query = new WJsonParser().parseQuery(json);
		condition = query.getCondition();
		condition.replaceColumn("location_type", "rf_alias.location_type");
		condition.replaceColumn("exe_source", "'gp'");
		condition.replaceColumn("res_site", "rf_alias.res_site");
		assertTrue("rf_alias.location_type != 'ld' AND NOT ('gp' = 'gp' AND rf_alias.res_site = 'APP' AND rf_alias.location_type NOT LIKE '%yd%')".equals(condition.getWhere()));

		json = "{\"where\": \"length(omgid)=46\", \"metric\": \"imp,click,income\", \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		query = new WJsonParser().parseQuery(json);
		condition = query.getCondition();
		condition.replaceColumn("omgid", "''");
		assertTrue("length('') = 46".equals(condition.getWhere()));

	}

	@Test
	public void testMetric() throws WCoreException {
		String json = "{\"where\": \"order_class='CMP' and loid=1\", \"metric\": [{\"name\":\"imp\", \"method\":\"min\", \"alias\":\"min_imp\"}], \"begin_day\": \"20170425\", \"end_day\": \"20170425\", \"user\": \"appuser\", \"table\": \"xinwen\", \"group_by\": \"year,date,city,loc_code\"}";
		RQuery query = new WJsonParser().parseQuery(json);
		RMetric[] ms = query.getMs();
		assertTrue(ms[0].getMethod().equalsIgnoreCase("MIN"));
		assertTrue(ms[0].getAlias().equalsIgnoreCase("MIN_imp"));
	}

}
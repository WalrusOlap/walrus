package org.pcg.walrus.core.parse;

import java.io.Serializable;

import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.util.CollectionUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.query.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* <code>WJsonParser</code> </br>
* parse json string to <code>ASTNode</code>
*/ 
public class WJsonParser extends WSqlParser implements Serializable {

	private static final long serialVersionUID = 303558062529668617L;

	private static final Logger log = LoggerFactory.getLogger(WJsonParser.class);

	/**
	 * @param logic json string
	 * {
	 *   "table": "view_name_1",
	 *   "group_by": "col_1,col_2...",
	 *   "metric": "metric_1,metric_2...", // default method sum
	 *   ||
	 *   "metric": [
	 *       {"name":"metric_1","method":"MIN|MAX|AVG|SUM..."},
	 *       {"name":"metric_2","method":"MIN|MAX|AVG|SUM..."},
	 *   ]
	 *   "filter": "col_1=val1 or ...",
	 *   "end_day": "1997-01-01 00:00:00",
	 *   "begin_day": "2019-12-31 23:59:59",
	 *   "order_by": "col_1...",
	 *   "limit": 100
	 * }
	 * @return <CODE>RQuery</CODE>
	 */
	@Override
	public RQuery parseQuery(String logic) throws WCoreException {
		// parse json
		JSONObject json = null;
		try {
			json = (JSONObject) new JSONParser().parse(logic);
		} catch (ParseException e) {
			throw new WCoreException("Invalid task json: " + logic);
		}
		// check
		validate(json);
		
		String tname = (String) json.get(CoreConstants.JSON_KEY_TABLE);
		String bday = String.valueOf(json.get(CoreConstants.JSON_KEY_BDAY));
		String eday = String.valueOf(json.get(CoreConstants.JSON_KEY_EDAY));
		RQuery query = null;
		try {
			query = new RQuery(tname, TimeUtil.stringToDate(bday), TimeUtil.stringToDate(eday));
		} catch (java.text.ParseException e1) {
			throw new WCoreException("parse date error: " + e1.getMessage());
		}
		// group by
		String colStr = (String) json.get(CoreConstants.JSON_KEY_COLS);
		String[] cols = colStr.replaceAll(" ", "").split(CoreConstants.JSON_DELIM);
		query.setCols(RColumn.toColumns(cols));
		// metrics
		RMetric[] ms = null;
		Object mstr = json.get(CoreConstants.JSON_KEY_MS);
		if(mstr instanceof String) // "metric": "metric_1,metric_2...",
			ms = RMetric.toMetrics(mstr.toString().replaceAll(" ", "").split(CoreConstants.JSON_DELIM));
		else { // [ {"name":"metric_1","method":"MIN|MAX|AVG|SUM..."}... ]
			JSONArray ary = (JSONArray) mstr;
			ms = new RMetric[ary.size()];
			for(int i=0; i<ary.size(); i++) {
				JSONObject m = (JSONObject) ary.get(i);
				String name = m.get("name").toString();
				RMetric r = new RMetric(name);
				if(m.containsKey("method")) {
					r.setMethod(m.get("method").toString());
					if(m.containsKey("params")) {
						r.setExType(CoreConstants.DERIVE_MODE_EXTEND);
						r.setParam(m.get("params").toString());
					} else r.setParam(name);
				}
				if(m.containsKey("alias")) r.setAlias(m.get("alias").toString());
				else r.setAlias(name);
				ms[i] = r;
			}
		}
		query.setMs(ms);
		// where
		String where = (String) json.get(CoreConstants.JSON_KEY_WHERE);
		try {
			ConditionVisitor visitor = new ConditionVisitor(where.trim());
			RCondition condition = visitor.parseCondition();
			query.setCondition(condition);
		} catch (Exception e) {
			log.error("parse sql error: " + e.getMessage());
			throw new WCoreException(e.getMessage());
		} 
		// parse limit and order by
		int limit = 0;
		if(json.containsKey(CoreConstants.JSON_KEY_LIMIT)) {
			long jsonLimit = (long) json.get(CoreConstants.JSON_KEY_LIMIT);
			limit = (int) jsonLimit;
		}
		// order by
		String orderBy = (String) json.get(CoreConstants.JSON_KEY_ORDERBY);
		query.setLimit(limit);
		query.setOrderBy(orderBy);
		// other params
		query.setParams(CollectionUtil.jsonToMap(json));
		return query;
	}
	
	// validate
	private void validate(JSONObject json) {
		assert (json.containsKey(CoreConstants.JSON_KEY_TABLE)) : "require key: " + CoreConstants.JSON_KEY_TABLE;
		assert (json.containsKey(CoreConstants.JSON_KEY_BDAY)) : "require key: " + CoreConstants.JSON_KEY_BDAY;
		assert (json.containsKey(CoreConstants.JSON_KEY_EDAY)) : "require key: " + CoreConstants.JSON_KEY_EDAY;
		assert (json.containsKey(CoreConstants.JSON_KEY_COLS)) : "require key: " + CoreConstants.JSON_KEY_COLS;
		assert (json.containsKey(CoreConstants.JSON_KEY_MS)) : "require key: " + CoreConstants.JSON_KEY_MS;
		assert (json.containsKey(CoreConstants.JSON_KEY_WHERE)) : "require key: " + CoreConstants.JSON_KEY_WHERE;
	}
}

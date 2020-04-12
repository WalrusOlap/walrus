package org.pcg.walrus.core.execution.job.stage;

import java.util.*;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.core.parse.aggregations.metrics.MetricFunction;
import org.pcg.walrus.core.udf.WUDF;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.query.RColumn;
import org.pcg.walrus.core.query.RCondition;
import org.pcg.walrus.core.query.RDict;
import org.pcg.walrus.core.query.RField;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RPartition;
import org.pcg.walrus.meta.pb.WFieldMessage;

import org.apache.commons.lang.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
* <code>EStage</code> </br>
* one stage for a walrus job
*/
public abstract class SqlStage extends BaseStage {

	private static final long serialVersionUID = -5967318792757281781L;

	// fact table
	protected transient static String FAKE_SQL = "SELECT COLS, MS FROM TNAME LEFT_JOIN WHERE FILTER GROUP BY GS";

	public SqlStage(WContext context, RPartition partition, Operator operator) {
		super(context, partition, operator);
	}

	// generate sql
	protected String generateSql(String tname) throws WCoreException {
		String ms = handleMetrics(partition.getMs());
		String[] fs = handleColumns(partition.getCols());
		String filter = handleCondition();
		String leftJoin = handleJoin();
		String sql = FAKE_SQL.replace("TNAME", tname + " " + CoreConstants.SQL_FACT_TABLE_ALIAS);
		sql = sql.replaceAll("COLS", fs[0]);
		sql = sql.replace("MS", ms);
		sql = sql.replace("FILTER", filter);
		sql = sql.replace("LEFT_JOIN", leftJoin);
		if(StringUtils.isEmpty(fs[1])) sql = sql.replaceAll("GROUP BY GS", "");
		else sql = sql.replaceAll("GS", fs[1]);
		return handleVariables(sql);
	}

	// build MS string
	protected String handleMetrics(RMetric[] ms) throws WCoreException {
		String[] mStrs = new String[ms.length];
		int index = 0;
		for(RMetric metric: ms){
			String metricStr = "";
			String name = metric.getName();
			String refer = CoreConstants.SQL_FACT_TABLE_ALIAS;
			WFieldMessage.WMetric wMetric = metric.getMetric();
			String method = metric.getMethod();
			String params = null;
			switch (metric.getDerivedMode()) {
			case CoreConstants.DERIVE_MODE_EXTEND:
				WFieldMessage.WUdf wudf = wMetric.getDerivedLogic();
				params = handleUdf(wudf);
				break;
			case CoreConstants.DERIVE_MODE_EXT1:
				WFieldMessage.WMetric joinM = metric.getJoinMetric();
				params = handleUdf(joinM.getDerivedLogic());
				break;
			case CoreConstants.DERIVE_MODE_VIRTUAL:
				params = metric.getDefaultVal().toString();
				break;
			case CoreConstants.DERIVE_MODE_JOIN:
				refer = metric.getRefer();
				params = String.format("%s.%s", refer, name);
				break;
			case CoreConstants.DERIVE_MODE_SELECT:
			default:
				params = String.format("%s.%s", refer, name);
				break;
			}
			MetricFunction fun = metric.getAggregateFuntion();
			if(fun != null)  mStrs[index++] = fun.selectSql(method, params, name);
			else mStrs[index++] = String.format("%s(%s) AS %s", method, params, name);
		}
		return Joiner.on(",").join(mStrs);
	}
	
	// build COLS string
	protected String[] handleColumns(RColumn[] cols) throws WCoreException {
		List<String> cStrs = new ArrayList<String>();
		List<String> gStrs = new ArrayList<String>();
		String[] fs = new String[2];
		for(RColumn dim: cols){
			String colStr = ""; // MS
			String gbStr = ""; // GS
			String name = dim.getName();
			String refer = dim.getRefer();
			WFieldMessage.WDimension dimension = dim.getDim();
			switch (dim.getDerivedMode()) {
			case CoreConstants.DERIVE_MODE_EXTEND:
				WFieldMessage.WUdf wudf = dimension.getDerivedLogic();
				String func = handleUdf(wudf);
				colStr = func + " AS " + name;
				gbStr = func;
				break;
			case CoreConstants.DERIVE_MODE_VIRTUAL:
				colStr = dim.getDefaultVal() + " AS " + name;
				break;
			case CoreConstants.DERIVE_MODE_SELECT:
			case CoreConstants.DERIVE_MODE_JOIN:
			default:
				colStr = refer + "." + name;
				gbStr = refer + "." + name;
				break;
			}
			if(!StringUtils.isEmpty(colStr)) cStrs.add(colStr);
			if(!StringUtils.isEmpty(gbStr)) gStrs.add(gbStr);
		}
		fs[0] = Joiner.on(",").join(cStrs);
		fs[1] = Joiner.on(",").join(gStrs);
		return fs;
	}

	// build FILTER string add alias to column
	protected String handleCondition()
			throws WCoreException {
		RCondition condition = partition.getCondition();
		for(RField field: condition.getColumns()) {
			String replaceField = field.getName();
			switch (field.getDerivedMode()) {
			case CoreConstants.DERIVE_MODE_EXTEND:
				RColumn dim = (RColumn) field;
				WFieldMessage.WUdf wudf = dim.getDim().getDerivedLogic();
				replaceField = handleUdf(wudf);
				break;
			case CoreConstants.DERIVE_MODE_VIRTUAL:
				replaceField = String.valueOf(field.getDefaultVal());
				break;
			case CoreConstants.DERIVE_MODE_JOIN:
			case CoreConstants.DERIVE_MODE_SELECT:
			default:
				replaceField = field.getRefer() + "." + field.getName();
				break;
			}
			condition.replaceColumn(field.getName(), replaceField);
		}
		return condition.getWhere();
	}
	
	// build LEFT_JOIN string, if not join, just group by
	protected String handleJoin() throws WCoreException {
		String leftJoin = "";
		if(partition.getDicts().size() > 0) {
			List<RDict> sortedDicts = new ArrayList<>(partition.getDicts());
			Collections.sort(sortedDicts);
			for(int k = 0; k < sortedDicts.size(); k++) {
				RDict dict = sortedDicts.get(k);
				String dictTable = dict.getTable();
				String dictName = dict.getDictName();
				String join = String.format(" %s join %s %s ON ", dict.getJoinType(), dictTable, dictName);
				List<String> conditions = Lists.newArrayList();
				String[] parentKeys = dict.getFactKeysList();
				String[] dictKeys = dict.getDictKeysList();
				for (int i = 0; i < parentKeys.length; i++) {
					String condition = "";
					boolean isCol = true;
					String leftParam = parentKeys[i];
					// handle left param
					for(RColumn col: partition.getCols()) {
						if(parentKeys[i].equals(col.getName())) {
							switch (col.getDerivedMode()) {
							case CoreConstants.DERIVE_MODE_VIRTUAL:
								isCol = false;
								leftParam = col.getDefaultVal() + "";
								break;
							case CoreConstants.DERIVE_MODE_EXTEND:
								WFieldMessage.WUdf wudf = col.getDim().getDerivedLogic();
								leftParam = handleUdf(wudf);
								isCol = false;
								break;
							default:
								break;
							}
						}
					}
					String rightParam = dictKeys[i];
					leftParam = handleVariables(leftParam);
					if(leftParam.contains(".") || leftParam.contains("(")) isCol = false;
					// dict alias
					RDict parent = dict.parent();
					String leftAlias = parent == null ? CoreConstants.SQL_FACT_TABLE_ALIAS : parent.getDict().getDictName();
					if(isCol) leftParam = leftAlias + "." + leftParam;
					condition = leftParam + "=" + dictName + "." + rightParam;
					conditions.add(condition);
				}
				join += Joiner.on(" AND ").join(conditions);
				leftJoin += join;
			}
		}
		return leftJoin;
	}
	
	// build udf string
	protected String handleUdf(WFieldMessage.WUdf wudf) throws WCoreException {
		String method = wudf.getMethodName();
		List<String> params = wudf.getParamsList();
		String udfStr = "";
		if(CoreConstants.UDF_METHOD_DEFAULT.equalsIgnoreCase(method)) {
			udfStr = params.get(0);
		} else {
			WUDF udf = null;
			try {
				udf = (WUDF) Class.forName(wudf.getClassName()).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new WCoreException("udf error: " + e.getMessage());
			}
			// register udf to sql context
			log.info(String.format("register udf %s...", method));
			if(udf != null) udf.register(context.getSqlContext());
			udfStr = method + "(" + Joiner.on(",").join(params) + ")";
		}
		return udfStr;
	}

	//default symbols: replace ${variable}
	protected String handleVariables(String str) {
		Map<String, Object> paraMap = new HashMap<String, Object>(){
			private static final long serialVersionUID = 1L;
			{
		        put("FACT_TABLE", CoreConstants.SQL_FACT_TABLE_ALIAS);
			}         
		};
		for(String k: paraMap.keySet()) {
			String key = "${" + k + "}";
			if (str.contains(k)) str = str.replace(key, String.valueOf(paraMap.get(k)));
		}
		return str;
	}
}

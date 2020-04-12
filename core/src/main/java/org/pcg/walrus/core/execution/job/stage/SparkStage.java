package org.pcg.walrus.core.execution.job.stage;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.execution.job.RStage;
import org.pcg.walrus.core.parse.aggregations.metrics.MetricFunction;
import org.pcg.walrus.core.plan.operator.Aggregator;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.query.RColumn;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RPartition;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.util.LogUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.pcg.walrus.meta.pb.WJobMessage;

import com.google.common.base.Joiner;

/**
* <code>SparkStage</code> </br>
* one stage for a spark job
*/
public class SparkStage extends SqlStage {

	private static final long serialVersionUID = -2713531310606626293L;

	public SparkStage(WContext context, RPartition partition, Operator operator) {
		super(context, partition, operator);
	}

	/**
	 * stage execution
	 * @return DataFrame
	 */
	public Dataset<Row> exe(WJobMessage.WJob wJob) throws WCoreException {
		Dataset<Row> dataFrame = null;
		switch (operator.getOperatorType()) {
		case CoreConstants.AST_OPER_AGGREGATOR:
			// union all chilren stages
			LogUtil.info(log, wJob.getId(), "execute aggregation stage!");
			Dataset<Row> df = null;
			for(RStage stage: stages) {
				Dataset<Row> ds = stage.exe(wJob);
				LogUtil.info(log, wJob.getId(), "[STAGE_AGGREGATION]dateset schema: " + ds.schema());
				df = df == null ? ds : df.union(ds);
			}
			// execute final aggregation
			Aggregator aggregator = (Aggregator) operator;
			String finalTablel = "walrus_aggregate_table_" + wJob.getId();
			context.getSqlContext().registerDataFrameAsTable(df, finalTablel);
			String finalSql = aggregateSql(finalTablel, aggregator.getMs(), aggregator.getCols(),
					aggregator.getOrderBy(), aggregator.getLimit());
			LogUtil.info(log, wJob.getId(), "execute aggregation sql: " + finalSql);
			dataFrame = context.getSqlContext().sql(finalSql);
			break;
		case CoreConstants.AST_OPER_COMBINER:
			LogUtil.info(log, wJob.getId(), "execute combine stage!");
			Dataset<Row> combines = null;
			for(RStage stage: stages) {
				Dataset<Row> ds = stage.exe(wJob);
				LogUtil.info(log, wJob.getId(), "[STAGE_COMBINER]dateset schema: " + ds.schema());
				combines = combines == null ? ds : combines.union(ds);
			}
			Combiner combiner = (Combiner) operator;
			String combineTable = "walrus_combine_table_" + wJob.getId();
			context.getSqlContext().registerDataFrameAsTable(combines, combineTable);
			String combineSql = aggregateSql(combineTable, combiner.getMs(), combiner.getCols(), null, 0);
			LogUtil.info(log, wJob.getId(), "execute combine sql: " + combineSql);
			dataFrame = context.getSqlContext().sql(combineSql);
			break;
		case CoreConstants.AST_OPER_UNION:
			// union all children stages
			LogUtil.info(log, wJob.getId(), "execute union stage!");
			for(RStage stage: stages) {
				Dataset<Row> ds = stage.exe(wJob);
				LogUtil.info(log, wJob.getId(), "[STAGE_UNION]dateset schema: " + ds.schema());
				dataFrame = dataFrame == null ? ds : dataFrame.union(ds);
			}
			break;
		case CoreConstants.AST_OPER_SELECT:
			// execute select stage
			LogUtil.info(log, wJob.getId(), "execute select stage!");
			String tablet = partition.getTname();
			String sql = generateSql(tablet);
			LogUtil.info(log, wJob.getId(), "execute sql: " + sql);
			dataFrame = context.getSqlContext().sql(sql);
			LogUtil.info(log, wJob.getId(), "[STAGE_SELECT]dateset schema: " + dataFrame.schema());
			break;
		default:
			break;
		}
		return dataFrame;
	}

	// final aggregateSql
	private String aggregateSql(String table, RMetric[] ms, RColumn[] columns, String orderBy, int limit) {
		String sum = "";
        for(RMetric c: ms){
        	String n = c.getName();
        	String alias = c.getAlias();
			// if has no param, set param metric itself
        	String m = c.getParam() != null && c.getParam().length() > 0 ? c.getParam() : n;
			MetricFunction func = c.getAggregateFuntion();
			if(func != null) sum += String.format(", %s", func.aggregateSql(m, alias));
			else sum += String.format(", %s(IF(%s is not null,%s,0)) AS %s", CoreConstants.METRIC_FUNCTION_SUM, m, m, alias);
        }
        int index = 0;
        String[] cols = new String[columns.length];
        for(RColumn c: columns) cols[index++] = c.getName();
        String colStr = Joiner.on(",").join(cols);
		String finalSql = "SELECT " + colStr + sum + " FROM " + table + " GROUP BY " + colStr;
		// order by
		if(StringUtils.isNotEmpty(orderBy)) finalSql += " ORDER BY " + orderBy;
		// limit 
		if(limit > 0) finalSql += " LIMIT " + limit;
		return handleVariables(finalSql);
	}
}

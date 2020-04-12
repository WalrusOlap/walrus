package org.pcg.walrus.core.parse;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.sql.DruidAstVisitor;
import org.pcg.walrus.common.sql.SqlUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <code>WSqlParser</code> </br>
 * parse simple sql with MYSQL LEX to <code>RQuery</code>:
 *
 * sql format:
 *  SELECT
 *      {col_1},{col_2}...,
 *      AGGREGATE_FUNC({metric_1}),AGGREGATE_FUNC({metric_2})...
 *  FROM
 *      {table_name}
 *  WHERE
 *      DATE_COLUMN between {begin_day} and {end_day} AND ...
 *  GROUP BY
 *      {col_1},{col_2}...
 *
 *  1 key word DATE_COLUMN, it's a virtual column which represent query date range.
 *  2 {begin_day}, {end_day} should be format {YYYYMMDD} or {YYYY-MM-DD HH:mm:ss}
 *
 *  e.g.
 *      select date,location_id,sum(pv) from t_daily_web_pv where DATE_COLUMN between 20190101 and 20190101 and location_id='loc_1' group by date,location_id
 *
*/ 
public class WSqlParser extends BaseParser {

	// date column
	private static String[] DATE_COLUMNS = new String[] {"DATE_COLUMN","DATE","TIME_COLUMN","TIME","TIMESTAMP_COLUMN","TIMESTAMP"};

	private static final Logger log = LoggerFactory.getLogger(WSqlParser.class);

	@Override
	public RQuery parseQuery(String sql) throws WCoreException {
		SQLStatement stmt = null;
		try {
			SQLStatementParser statParser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.MYSQL);
			List<SQLStatement> stmtList = statParser.parseStatementList();
			if(stmtList.size() != 1) throw new WCoreException(String.format("Multiple queries is not supported: %s", sql));
			stmt = stmtList.get(0);
		} catch (ParserException e) {
			throw new WCoreException(String.format("invalid sql: %s[%s]", sql, e.getMessage()));
		}
		// validate sql
		validate(stmt);
		// extract table, date range, columns, metrics, where  from sql
        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        SQLSelectQuery select = selectStmt.getSelect().getQuery();
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) select;
        // table
        SQLExprTableSource from = (SQLExprTableSource) queryBlock.getFrom();
        String table = from.getName().getSimpleName();
        // where
        SQLExpr where = queryBlock.getWhere();
		ConditionVisitor visitor = new ConditionVisitor(SQLUtils.toSQLString(where));
		where.accept(visitor);
		String errorInfo = String.format("missing date column condition: %s\ncorrect format: DATE_COLUMN between {begin_day} and {end_day} AND ...", where);
		assert visitor.bday != null : errorInfo;
		assert visitor.eday != null : errorInfo;
		RQuery query = new RQuery(table, visitor.bday, visitor.eday);
		query.setCondition(visitor.parseCondition());
		// columns, metric
		List<RColumn> cols = new ArrayList<>();
		List<RMetric> ms = new ArrayList<>();
		for(SQLSelectItem col: queryBlock.getSelectList()) handleSelectItem(col, cols, ms);
		query.setCols(cols.toArray(new RColumn[cols.size()]));
		query.setMs(ms.toArray(new RMetric[ms.size()]));
		// limit
		SQLLimit limit = queryBlock.getLimit();
		if(limit != null) {
			SQLExpr rowCount = limit.getRowCount();
			query.setLimit(((SQLIntegerExpr) rowCount).getNumber().intValue());
		}
		// order by
		SQLOrderBy orderBy = queryBlock.getOrderBy();
		if(orderBy != null) {
			List<String> orderByCols = new ArrayList<>();
			for(SQLSelectOrderByItem item: orderBy.getItems()) orderByCols.add(item.getExpr().toString());
			query.setOrderBy(Joiner.on(",").join(orderByCols));
		}
		// empty params
		query.setParams(new HashMap<>());
		return query;
	}

	/**
	 * add columns and metrics
	 */
	private void handleSelectItem(SQLSelectItem col, List<RColumn> cols, List<RMetric> ms) throws WCoreException {
		// alias
		SQLExpr expr = col.getExpr();
		RColumn c = null;
		RMetric m = null;
		String alias = col.getAlias();
		if(expr instanceof SQLIdentifierExpr) {
			String name = ((SQLIdentifierExpr) expr).getName();
			c = new RColumn(name);
		} else if(expr instanceof SQLLiteralExpr) {
			SQLDataType dataType = expr.computeDataType();
			c = new RColumn(colName(expr, alias), dataType.getName());
			c.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
			if(expr instanceof SQLNumericLiteralExpr)
				c.setDefaultVal(expr.toString());
			else c.setDefaultVal(String.format("'%s'", expr.toString()));
		} else if (expr instanceof SQLCaseExpr) {
			// TODO supported case when
			throw new WCoreException(String.format("unsupported syntax: CASE WHEN"));
		} else if (expr instanceof SQLAggregateExpr) { //
			String method = ((SQLAggregateExpr) expr).getMethodName();
			SQLAggregateOption option = ((SQLAggregateExpr) expr).getOption();
			if(option != null) {
				if("DISTINCT".equalsIgnoreCase(option.name())) {
					method = CoreConstants.METRIC_FUNCTION_DISTINCT;
				} else throw new WCoreException(String.format("unsupported syntax: %s", option));
			}
			List<SQLExpr> args = ((SQLAggregateExpr) expr).getArguments();
			if(args.size() > 1) throw new WCoreException(String.format("unsupported syntax: %s", expr));
			SQLExpr arg = args.get(0);
			if(arg instanceof SQLIdentifierExpr) {
				m = new RMetric(colName(arg, alias));
				m.setMethod(method);
				m.setParam(((SQLIdentifierExpr) arg).getName());
			}
			// TODO support metric binary op
//			else if (arg instanceof SQLBinaryOpExpr) {
//				m = handleBinaryMetric((SQLBinaryOpExpr) arg, method, alias);
//			}
			else throw new WCoreException(String.format("unsupported syntax: %s", expr));
		} else if (expr instanceof SQLBinaryOpExpr) { //
			// if Aggregate op, add metric
			if(isAggregate((SQLBinaryOpExpr) expr)) {
				checkMethod((SQLBinaryOpExpr) expr);
				m = new RMetric(colName(expr, alias));
				m.setExType(CoreConstants.DERIVE_MODE_EXTEND);
				m.setMethod(CoreConstants.METRIC_FUNCTION_MATH_EXP);
				// remove aggregate function from sql
				expr.accept(new DruidAstVisitor() {
					@Override
					protected void visitBinaryLeaf(SQLBinaryOpExpr expr) {
						SQLExpr left = expr.getLeft();
						SQLExpr right = expr.getRight();
						if(left instanceof SQLAggregateExpr) expr.setLeft(replaceAggregator((SQLAggregateExpr) left));
						if(right instanceof SQLAggregateExpr) expr.setRight(replaceAggregator((SQLAggregateExpr) right));
					}

					private SQLExpr replaceAggregator(SQLAggregateExpr agg) {
						return agg.getArguments().get(0);
					}

					@Override
					protected void visitIn(SQLInListExpr expr) {}
					@Override
					protected void visitBetween(SQLBetweenExpr expr) {}
				});
				m.setParam(expr.toString());
			}
			else { // if group by, add column
				c = new RColumn(colName(expr, alias));
				c.setExType(CoreConstants.DERIVE_MODE_EXTEND);
				c.setParam(expr.toString());
			}
		} else if (expr instanceof SQLMethodInvokeExpr) { //
			c = new RColumn(colName(expr, alias));
			c.setExType(CoreConstants.DERIVE_MODE_EXTEND);
			c.setParam(expr.toString());
		}
		else throw new WCoreException(String.format("unsupported syntax: %s", expr));
		if(c != null) {
			if(alias != null && alias.length() > 0) c.setAlias(alias);
			cols.add(c);
		}
		if(m != null) {
			if(alias != null && alias.length() > 0) m.setAlias(alias);
			ms.add(m);
		}
	}

	/**
	 * if is aggregate operator
	 */
	private boolean isAggregate(SQLBinaryOpExpr arg) {
		List<SQLObject> children = arg.getChildren();
		if(loopChildren(children)) return true;
		else return false;
	}
	private boolean loopChildren(List<SQLObject> children) {
		for(SQLObject obj: children) {
			if(obj instanceof SQLAggregateExpr) return true;
			else if(obj instanceof SQLBinaryOpExpr)
				return loopChildren(((SQLBinaryOpExpr) obj).getChildren());
		}
		return false;
	}

	/**
	 * only sum method is supported
	 */
	private void checkMethod(SQLBinaryOpExpr expr) throws WCoreException {
		List<SQLObject> children = expr.getChildren();
		for(SQLObject obj: children) {
			if(obj instanceof SQLAggregateExpr) {
				if(! CoreConstants.METRIC_FUNCTION_SUM.equalsIgnoreCase(((SQLAggregateExpr) obj).getMethodName()))
					throw new WCoreException(String.format("unsupported syntax: %s", expr));
				if(obj instanceof SQLBinaryOpExpr) checkMethod((SQLBinaryOpExpr) obj);
			}
		}
	}

	/**
	 * column name
	 */
	private String colName(SQLExpr expr, String alias) {
		return alias != null && alias.length() > 0 ? alias : expr.toString();
	}

	/**
	 * visit where and extract date column
	 */
	protected class ConditionVisitor extends DruidAstVisitor {

		private String where;
		private Date bday;
		private Date eday;

		public ConditionVisitor(String where) {
			this.where = where;
		}

		// parse condition
		public RCondition parseCondition() {
			Set<RField> columns = Sets.newHashSet();
			if(StringUtils.isEmpty(where)) where = "1=1";
			// columns
			List<SQLStatement> stmtList = SQLUtils.parseStatements(String.format("SELECT COL_A FROM TABLE_A WHERE %s", where), JdbcConstants.MYSQL);
			SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
			MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
			stmt.accept(visitor);
			List<TableStat.Condition> conditions = visitor.getConditions();
			for(TableStat.Condition con: conditions) {
				TableStat.Column col = con.getColumn();
				columns.add(new RField(col.getName()) {
					private static final long serialVersionUID = 1L;
					@Override
					public String getDerivedMode() {
						return null;
					}

					@Override
					public List<String> getParamList() {
						return null;
					}

					@Override
					public RField copy() {
						return null;
					}
				});
			}
			// expr
			SQLSelect select = stmt.getSelect();
			SQLSelectQueryBlock block = (SQLSelectQueryBlock) select.getQuery();
			SQLExpr expr = block.getWhere();
			RCondition condition = new RCondition(columns, where);
			condition.setExpr(expr);
			return condition;
		}


		@Override
		protected void visitBinaryLeaf(SQLBinaryOpExpr expr) {}

		@Override
		protected void visitIn(SQLInListExpr expr) {}

		/**
		 * extract bday, eday and replace date condition to 1=1
		 */
		@Override
		protected void visitBetween(SQLBetweenExpr expr) {
			String column = expr.getTestExpr().toString();
			String dateColumn = column.toUpperCase();
			if(Arrays.asList(DATE_COLUMNS).contains(dateColumn)) {
				try {
					bday = TimeUtil.stringToDate(expr.getBeginExpr().toString());
					eday = TimeUtil.stringToDate(expr.getEndExpr().toString());
				} catch (Exception e) {
					log.error(String.format("invalid date format: %s - %s", expr.getBeginExpr().toString(), expr.getEndExpr().toString()));
				}
				SQLExpr whereExpr = SqlUtil.toSqlExpr(where);
				String replace = String.format("%s BETWEEN %s AND %s", dateColumn, expr.getBeginExpr().toString(), expr.getEndExpr().toString());
				where = SQLUtils.toSQLString(whereExpr).replaceAll(String.format("(?i)%s", replace), "1=1");
			}
		}
	}

	/**
     * only simple select statement is supported.
	 * unsupported and unnecessary operators:
	 *  1 DDL, show, explain, call...
	 *  2 union, join, sub_query
	 */
	private void validate(SQLStatement stmt) throws WCoreException {
		if(!(stmt instanceof SQLSelectStatement))
			throw new WCoreException(String.format("Only select operator is supported: %s", stmt));
		SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
		SQLSelectQuery select = selectStmt.getSelect().getQuery();
		// union
		if(select instanceof SQLUnionQuery)
			throw new WCoreException(String.format("operator %s is not supported", select));
		else if(select instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) select;
            SQLTableSource table = query.getFrom();
            if(table instanceof SQLSubqueryTableSource)
                throw new WCoreException(String.format("subquery is not supported: %s", table));
            else if(table instanceof SQLJoinTableSource)
                throw new WCoreException(String.format("operator join is not supported: %s", table));
            else if(table instanceof SQLUnionQueryTableSource)
                throw new WCoreException(String.format("operator union is not supported: %s", table));
            else if(table instanceof SQLLateralViewTableSource)
                throw new WCoreException(String.format("operator view is not supported: %s", table));
            else if(!(table instanceof SQLExprTableSource))
                throw new WCoreException(String.format("query is not supported: %s", table));
		} else throw new WCoreException(String.format("unsupported query: %s", select));
	}
}

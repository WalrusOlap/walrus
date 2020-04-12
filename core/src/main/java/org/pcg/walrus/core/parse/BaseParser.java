package org.pcg.walrus.core.parse;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.parse.aggregations.Aggregation;
import org.pcg.walrus.core.parse.aggregations.AggregationBuilder;

import org.pcg.walrus.core.parse.choose.ChooserFactory;
import org.pcg.walrus.core.parse.choose.IChooser;

import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.core.plan.LogicalPlan;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.MetaUtils;

import org.pcg.walrus.meta.WMeta;
import org.pcg.walrus.meta.partition.PartitionSplitor;
import org.pcg.walrus.meta.partition.SPartition;
import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.meta.pb.WViewMessage;
import org.pcg.walrus.common.util.CollectionUtil;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.util.TimeUtil;

import org.apache.commons.lang.exception.ExceptionUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.node.ASTVisitor;
import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.plan.operator.Unifier;

import org.pcg.walrus.core.query.*;
import org.pcg.walrus.meta.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseParser</br>
 * 	 parse a query to a ASTree for calculatation framework
 */
public abstract class BaseParser extends ParseValidator {

	private static final Logger log = LoggerFactory.getLogger(BaseParser.class);

	// parse logic to <CODE>RQuery</CODE>
	public abstract RQuery parseQuery(String logic) throws WCoreException;

	/**
	 * parse query to <CODE>ASTree</CODE>
	 * 
	 * @param meta
	 *            <CODE>WMeta</CODE>
	 * @param logic
	 *            json,sql...
	 * @return query plan
	 * @throws WCoreException
	 */
	public LogicalPlan parse(WMeta meta, String logic, String level, long jobId) throws WCoreException {
		ASTree tree = null;
		// build ast for each cluster
		RQuery query = parseQuery(logic);
		Date bday = query.getBday();
		Date eday = query.getEday();
		// meta
		Map<String, MViewNode> views = null;
		Map<String, MTableNode> tables = null;
		Map<String, MTableNode> dicts = null;
		try {
			views = meta.getViews();
		    tables = meta.getTables();
			dicts = meta.getDicts();
		} catch (WMetaException e) {
			LogUtil.error(log, jobId, "load meta error: " + ExceptionUtils.getFullStackTrace(e));
			throw new WCoreException("load meta error:" + e.getMessage());
		}
	    // validate
		validateQuery(query);
		query.setLevel(level);
		LogUtil.info(log, jobId, "parse query: " + query);
		String tname = query.getTname();
		// build ast
		Aggregation aggregation = null;
		ASTNode son = null;
		// if table, simply create a ASTree with selector operator
		if(tables.containsKey(tname)) {
			LogUtil.debug(log, jobId, "query table: " + tname);
			MTableNode tNode = tables.get(tname);
			// build aggregation
			try {
				aggregation = AggregationBuilder.buildAggregation(query, tNode.dims(bday, eday, level, true),
						tNode.metrics(bday, eday, level, true));
			} catch (Exception e) {
				LogUtil.error(log, jobId, "buildAggregation error: " + ExceptionUtils.getFullStackTrace(e));
				throw new WCoreException("build Aggregation error:" + e.getMessage());
			}
			// table
			RTable table = new RTable(tNode, null, dicts);
			son = parseTable(table, query, aggregation.leaf(), level, true, jobId);
		}
		// if view, traverse view and create a ast
		else if (views.containsKey(tname)) {
			LogUtil.debug(log, jobId, "query view: " + tname);
			MViewNode view = views.get(tname);
			// validate view
			validateView(view, query, tables, dicts, level, jobId);
			// add view condition
			for(String filter: view.getView().getFiltersList())
				query.getCondition().addCondition(filter);
			// build aggregation
			try {
				aggregation = AggregationBuilder.buildAggregation(query,
						MetaUtils.getViewDims(view.getView(), tables, dicts, bday, eday, level),
						MetaUtils.getViewMetrics(view.getView(), tables, dicts, bday, eday, level));
			} catch (Exception e) {
				LogUtil.error(log, jobId, "buildAggregation error: " + ExceptionUtils.getFullStackTrace(e));
				throw new WCoreException("build Aggregation error:" + e.getMessage());
			}
			// create view node
			son = parseView(view.getView(), query, tables, dicts, aggregation.leaf(), jobId);
		} else throw new WCoreException("Unknown view or table: " + tname);
		if(son == null) throw new WCoreException("parse query error: empty plan!");
		aggregation.leaf().appendChild(son);
		tree = new ASTree(aggregation.root());
		return new LogicalPlan(tree, tname, bday, eday, query.getParams());
	}

	/**
	 * ASTNode(Unifier):
	 *    --ASTNode(Unifier)
	 *         --ASTNode(Selector)
	 *         --ASTNode(Selector)
	 *    --ASTNode(Unifier)
	 *         --ASTNode(Unifier)
	 *         	   --ASTNode(Selector)
	 *         	   --ASTNode(Selector)
	 *    ...
	 */
	private ASTNode parseView(WViewMessage.WView view, RQuery query,
                              Map<String, MTableNode> tables, Map<String, MTableNode> dicts,
                              ASTNode parent, long jobId) throws WCoreException {
		ASTNode node = null;
		String level = query.getLevel();
		RCondition condition = query.getCondition();
		// partition info
		List<WViewMessage.WIgnoreMsg> ignores = view.getIgnoreMsgList();
		// if no necessary query this view , continue
		if(ignoreView(ignores, condition, jobId, view, parent)) return null;

		// if leaf, choose the best table and return a selector node
		if(view.getUnionsCount() == 0) {
			List<RTable> ts = Lists.newArrayList();
			for(MViewTableNode t: MetaUtils.getViewTables(view)) {
				MTableNode table = tables.get(t.getName());
				if(table != null) {
					// find all join dict in parent node
					RTable r = new RTable(table, searchParentDict(parent), dicts);
					r.setPriority(t.getLevel());
					LogUtil.debug(log, jobId, "add table: " + r);
					ts.add(r);
				}
			}
			// choose table based on {choose.class}
			String strategy = System.getProperty("walrus.choose.strategy", "priority");
			IChooser chooser = ChooserFactory.loadChooser(strategy);
			RTable table = chooser.apply(ts, query);
			if(table == null) throw new WCoreException("no table be chosen for view: " + view.getName());
			node = parseTable(table, query, parent, level, false, jobId);
		} 
		// if not leaf, choose dict tables and children views
		else {
			String vName = view.getName();
			Operator union = new Unifier(view.getUnionMode(), view.getJoinsList());
			node = new ASTNode(union);
			Date bday = query.getBday();
			Date eday = query.getEday();
			// select view
			List<WViewMessage.WView> views = view.getUnionsList();
			String unionMode = view.getUnionMode();
			switch (unionMode) {
			// union all child
			case CoreConstants.OPER_UNION_SIMPLE:
				for(WViewMessage.WView v: views) {
					ASTNode child = parseView(v,  query, tables, dicts, node, jobId);
					LogUtil.debug(log, jobId, vName + " add simple view: " + child);
					if(child != null) node.appendChild(child);
				}
				break;
			// choose necessary views
			case CoreConstants.OPER_UNION_SUPPLEMENT:
				for(WViewMessage.WView v: views) {
					ASTNode child = null;
					// if no necessary query this view , continue
					if(ignoreView(v.getIgnoreMsgList(), condition, jobId, v, child)) continue;

					Set<String> vms = MetaUtils.getViewMs(v, tables, dicts, bday, eday, level);
					addDefaultMetrics(vms);
					// if metric not in this view, ignore view
					LogUtil.info(log, jobId, "view metrics: " + vms);
					Set<String> vCols = MetaUtils.getViewColumns(v, tables, dicts, bday, eday, level);
					LogUtil.info(log, jobId, "view columns: " + vCols);
                    if(vms.size() == 0 || vCols.size() == 0) continue;

					// set ignore metric to virtual
                    Set<String> ms = new HashSet<String>(Arrays.asList(RField.toFieldArray(query.getMs())));
					Set<String> metrics = CollectionUtil.intersection(vms, ms);
					LogUtil.info(log, jobId, v.getName() + " intersection metrics: " + metrics);
					Set<String> diffMs = CollectionUtil.diffSets(vms, ms);
					LogUtil.info(log, jobId, v.getName() + " diff metrics: " + diffMs);
					// if extend metric, check metric column

					if (!metrics.isEmpty()) {
						RQuery q = query.copy(bday, eday);
						if(diffMs.size() > 0) q.setVirtual(diffMs);
						child = parseView(v, q, tables, dicts, node, jobId);
					}
					if(child != null) node.appendChild(child);
				}
				break;
			// split query to sub queries
			case CoreConstants.OPER_UNION_SPLIT:
				// TODO
			default:
				break;				
			}
			// view child should not be null
			if(node == null || node.childNum() == 0) return null;
		}
		return node;
	}

	/**
	 * if ignore this view:
	 * 	exclusive check: 排他性检查，如果指定分区条件，忽略同级下其他view <br>
	 * 	eq check: 当前sql满足相等条件,忽略当前view <br>
	 * 	neq check: 当前sql满足不相等条件,忽略当前view <br>
	 * 	violence check: 暴力检查，一旦出现此column，忽略当前view <br>
	 */
	private boolean ignoreView(List<WViewMessage.WIgnoreMsg> infos,
			RCondition condition, long jobId, WViewMessage.WView view, ASTNode parent) {
		// if query filter a partition key, ignore this view
		for(WViewMessage.WIgnoreMsg info: infos) {
			String pType = info.getType();
			String partitionKey = info.getColumn();
			String partitionVal = info.getValue();			
			switch (pType) {
			// exclusive check, if query specified a partition key, ignore other view
			case CoreConstants.PARTITION_IGNORE_TYPE_EXCELUSIVE:
				if(condition.match(partitionKey + "!=" + partitionVal)) {
					LogUtil.debug(log, jobId, "(" + partitionKey + "!=" + partitionVal + ")ignore view: " + view.getName());
					return true;
				}
				break;
			case CoreConstants.PARTITION_IGNORE_TYPE_EQ:
				if(condition.match(partitionKey + "=" + partitionVal)) {
					LogUtil.debug(log, jobId, "(" + partitionKey + "!=" + partitionVal + ")ignore view: " + view.getName());
					return true;
				}
				break;
			// if negative check, 
			case CoreConstants.PARTITION_IGNORE_TYPE_NEQ:
				if(condition.match(partitionKey + "!=" + partitionVal)) {
					LogUtil.debug(log, jobId, "(" + partitionKey + "!=" + partitionVal + ")ignore view: " + view.getName());
					return true;
				}
				break;
			// if negative check,
			case CoreConstants.PARTITION_IGNORE_TYPE_VIOLENCE:
				Set<String> fileds = new HashSet<String>();
				for(RField filed: condition.getColumns()) fileds.add(filed.getName());
				if(fileds.contains(partitionKey)) return true;
				break;
			default:
				break;
			}
		}
		return false;
	}
	/**
	 * parse <CODE>RQuery</CODE> to a ASTNode with operator <CODE>Selector</CODE>
	 */
	private ASTNode parseTable(RTable rTable, RQuery query, ASTNode parent, String cluster, boolean validate, long jobId)
			throws WCoreException {
		// check query
		if(validate) validateTable(rTable, query, jobId);
		
		MTableNode tableNode = rTable.getTableNode();
		Date bday = query.getBday();
		Date eday = query.getEday();
		WTableMessage.WTable table = tableNode.getTable();
		String pMode = table.getPartitionMode();
		Selector selector = new Selector(bday, eday);
		
		List<RPartition> partitions = splitPartitions(pMode, bday, eday);
		for(RPartition partition: partitions) {
			Date pBday = partition.getBday();
			Date pEday = partition.getEday();
			// check partition
			String partitionName = partition.getPartition();
			// when partition not found: if validate throw exception, else continue
			if(!tableNode.getPartitions().containsKey(partitionName) && validate)
				throw new WCoreException("meta error: invalid partition " + partitionName);
			if(!tableNode.getPartitions().containsKey(partitionName) && !validate)
				continue;
			
			MPartitionNode partitionNode = tableNode.getPartitions().get(partitionName);
			WTableMessage.WPartitionTable pTable = partitionNode.getPartition();
			if(MetaConstants.META_STATUS_VALID != pTable.getIsValid())
				throw new WCoreException("meta error: invalid partition " + partitionName);
			
			// check base table
			WTableMessage.WBaseTable bTable = partitionNode.getBaseTableNode().getTable();
			if(MetaConstants.META_STATUS_VALID != bTable.getIsValid())
				throw new WCoreException("meta error: invalid base table " + partitionNode.getBaseTableNode().getName());
			try {
				Date bBday = TimeUtil.stringToDate(bTable.getStartTime());
				Date bEday = TimeUtil.stringToDate(bTable.getEndTime());
				if(validate && (pBday.before(bBday) || pEday.after(bEday))) 
					throw new WCoreException(table.getTableName() + "[" + partitionName
							+ "]unsupported calculate base day range[" + bBday
							+ "-" + bEday + "]: " + pBday + "-" + pEday);
			} catch (ParseException e) {
				throw new WCoreException("parse date error: "  + e.getMessage());
			}

			// check tablet
			MTabletNode tablet = partitionNode.getBaseTableNode().getTabletNode();
			if(MetaConstants.META_STATUS_VALID != tablet.getTablet().getIsValid())
				throw new WCoreException("meta error: invalid tablet " + tablet.getName());

			// path, format, schema
			partition.setFormat(tablet.getTablet().getFormat());
			partition.setPath(tablet.getTablet().getPath());
			partition.setSchema(tablet.getTablet().getSchema());
			// add column join dict
			Set<RDict> dicts = rTable.columnDicts(query, pBday, pEday);
			LogUtil.info(log, jobId, "join dicts: " + dicts);

			// cols 
			List<WFieldMessage.WDimension> dims = pTable.getDimensionsList();
			List<WFieldMessage.WMetric> ms = pTable.getMetricsList();
			RColumn[] columns = new RColumn[query.getCols().length];
			int index = 0;
			for(RColumn c: query.getCols()) {
				RColumn sc = serachColumn(dims, dicts, c);
				columns[index++] = sc;
			} 
			columns = rTable.addColumnJoinLogic(dicts, columns, dims);
			partition.setCols(columns);
			// ms
			RMetric[] metrics = new RMetric[query.getMs().length];
			index = 0;
			for(RMetric m: query.getMs()) metrics[index++] = serachMetric(ms, m);
			
			// add metrics join dicts
			Set<RDict> metricDicts = rTable.metricDicts(metrics, pBday, pEday);
			dicts.addAll(metricDicts);
			// dicts
			partition.setDicts(dicts);
			
			// add metrics join logic
			metrics = rTable.addMetricJoinLogic(metrics, pBday, pEday);
			partition.setMs(metrics);
			
			// condition
			RCondition contition = query.getCondition().copy();
			Set<RField> fields = Sets.newHashSet();
			for(RField f: contition.getColumns()) {
				RColumn column = serachColumn(dims, dicts, new RColumn(f.getName()));
				if(column != null) fields.add(column);
				// else virtual column
				else {
					f.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
					fields.add(f);
				}
				RMetric m = new RMetric(f.getName());
				m.setMethod(CoreConstants.METRIC_FUNCTION_SUM);
				RMetric metric = serachMetric(ms, m);
				if(metric != null) fields.add(metric);
				// else virtual metric
				else {
					f.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
					fields.add(f);
				}
			}
			contition.setColumns(fields);
			partition.setCondition(contition);
			// if partition mode in A,Y,M: add condition {date between bday and eday}
			if(MetaConstants.PARTITION_MODE_A.equalsIgnoreCase(pMode) || MetaConstants.PARTITION_MODE_Y.equalsIgnoreCase(pMode)
				|| MetaConstants.PARTITION_MODE_M.equalsIgnoreCase(pMode))
				contition.addCondition(String.format("%s BETWEEN %s and %s", table.getDateColumn(),
						TimeUtil.DateToString(bday), TimeUtil.DateToString(eday)));
			// tname, limit , order by
			partition.setTname(tablet.getName());
			// record num
			partition.setRecordNum(tablet.getTablet().getRecordNum());
			selector.addPartition(partition);
			LogUtil.debug(log, 0, "parse selector: " + selector);
		}
		// ast node with selector
		return new ASTNode(selector);
	}

	// search column from table and dict
	private RColumn serachColumn(List<WFieldMessage.WDimension> dims, Set<RDict> dicts, RColumn queryCol) {
		if(CoreConstants.DERIVE_MODE_EXTEND.equalsIgnoreCase(queryCol.getDerivedMode())
			|| CoreConstants.DERIVE_MODE_VIRTUAL.equalsIgnoreCase(queryCol.getDerivedMode()))
			return queryCol;
		String colName = queryCol.getName();
		RColumn column = null;
		boolean found = false;
		for(WFieldMessage.WDimension d: dims) {
			if(d.getName().equals(colName)) {
				column = new RColumn(d);
				column.setRefer(CoreConstants.SQL_FACT_TABLE_ALIAS);
				found = true;
				break;
			}
		}
		// join from dict
		if(!found) {
			for(RDict dict: dicts) {
				for(RColumn col: dict.getJoinColumns()) {
					if(col.getName().equals(colName)) {
						column = col.copy();
						column.setRefer(dict.getDict().getDictName());
						break;
					}
				}
			}
		}
		// if not found, set virtrual
		if(column == null) {
			column = new RColumn(colName, CoreConstants.COLUMN_TYPE_STRING);
			column.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
		}
		return column;
	}
	
	// search metric
	private RMetric serachMetric(List<WFieldMessage.WMetric> ms, RMetric m) {
		// virtual metric
		RMetric metric = null;
		if(CoreConstants.DERIVE_MODE_VIRTUAL.equals(m.getDerivedMode())
			|| CoreConstants.DERIVE_MODE_EXTEND.equals(m.getDerivedMode())) {
			metric = m;
		} else {
			for(WFieldMessage.WMetric wm: ms) {
				if(wm.getName().equals(m.getName())) {
					RMetric rm = new RMetric(wm);
					// join
					if(CoreConstants.DERIVE_MODE_JOIN.equals(wm.getDerivedMode())) {
						rm.setRefer(wm.getDict().getDictName());
					}
					metric = rm;
					break;
				}
			}
		}
		// if not found, set virtrual
		if(metric == null) {
			metric = new RMetric(m.getName(), CoreConstants.COLUMN_TYPE_DOUBLE);
			metric.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
			metric.setMethod(CoreConstants.METRIC_FUNCTION_SUM);
		}
		// original method
		if(m.getAggregateFuntion() != null) metric.setAggregateFuntion(metric.getAggregateFuntion());
		if(m.getMethod() != null) metric.setMethod(m.getMethod());
		return metric;
	}

	/*
	 * split query into partitions base on partition mode
	 */
	private List<RPartition> splitPartitions(String pMode, Date bday, Date eday)
			throws WCoreException {
		List<RPartition> ps = Lists.newArrayList();
		List<SPartition> splits = PartitionSplitor.splits(pMode, bday, eday);
		for(SPartition split: splits) {
			RPartition p = new RPartition(pMode, split.getBday(),
					split.getEday(), split.getName());
			ps.add(p);
		}
		return ps;
	}
	
	// return all dict in pardent node
	// ! DO NOT change the Pointer of node
	private Set<WFieldMessage.WDict> searchParentDict(ASTNode node){
		Set<WFieldMessage.WDict> dicts = Sets.newHashSet();
		DictSearcher visitor = new DictSearcher(dicts);
		node.traverseUp(visitor);
		return dicts;
	}

	// add dict if visit a unifer
	private class DictSearcher implements ASTVisitor {
		private Set<WFieldMessage.WDict> dicts;
		public DictSearcher(Set<WFieldMessage.WDict> dicts) {
			this.dicts = dicts;
		}		
		@Override
		public void visit(ASTNode node) {
			Operator op = node.getOperator();
			if(CoreConstants.AST_OPER_UNION.equals(op.getOperatorType())) {
				Unifier union = (Unifier) op;
				dicts.addAll(union.getDicts());
			}
		}
	}

	// add view default metric
	private void addDefaultMetrics(Set<String> metrics) {
		metrics.add(CoreConstants.QUERY_METRIC_DEFAULT_COUNT);
	}
}

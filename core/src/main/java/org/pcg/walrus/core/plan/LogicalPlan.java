package org.pcg.walrus.core.plan;

import java.io.Serializable;
import java.util.*;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.node.ASTVisitor;

import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.core.plan.operator.Aggregator;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.query.RColumn;
import org.pcg.walrus.core.query.RDict;
import org.pcg.walrus.core.query.RField;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RPartition;
import org.pcg.walrus.meta.MetaConstants;

/**
* <code>LogicalPlan</code> </br>
* logical plan for walrus job </br>
* 
* astree:  </br>
* 	Unifier: timeline </br>
		Unifier: supplement </br>
			Selector: </br>
		Unifier: supplement </br>
			Selector: </br>
*/ 
public class LogicalPlan implements Serializable {

	private static final long serialVersionUID = -1138947090385106648L;

	private ASTree tree;
	private Map<String, Object> params;

	private long size = 0;
	private String table;
	private Date bday;
	private Date eday;
	private String partitionMode;

	private Set<String> models = new HashSet<String>();
	private Set<String> tables = new HashSet<String>();
	private Set<String> dicts = new HashSet<String>();
	private Set<String> columns = new HashSet<String>();

	public LogicalPlan(ASTree tree, String table, Date bday, Date eday, Map<String, Object> params) {
		this.tree = tree;
		this.table = table;
		this.bday = bday;
		this.eday = eday;
		this.params = params;
		readTree();
	}

	private void readTree(){
		try {
			if(tree != null) tree.getRoot().traverseDown(new SelectorVisitor());
		} catch (Exception e){
			// do nothing
		}
	}

	public ASTree getPlan() {
		return tree;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public long getDataSize() {
		return size;
	}
	
	public Set<String> getModels() {
		return models;
	}

	public Set<String> getTables() {
		return tables;
	}
	
	public Set<String> getDicts() {
		return dicts;
	}
	
	public Set<String> getColumns() {
		return columns;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("tables: " + tables + "\n");
		sb.append("dicts: " + dicts + "\n");
		sb.append("plan: " + tree + "\n");
		return sb.toString();
	}

	public String getTable() {
		return table;
	}

	public Date getBday() {
		return bday;
	}

	public Date getEday() {
		return eday;
	}

	public String partitionMode() {
		return partitionMode;
	}

	/**
	 *  visit all selector in astree and get all need information 
	 */
	private class SelectorVisitor implements ASTVisitor {
		@Override
		public void visit(ASTNode node) {
			Operator op = node.getOperator();
			if (CoreConstants.AST_OPER_SELECT.equals(op.getOperatorType())) {
				Selector selector = (Selector) op;
				for(RPartition partition: selector.getPartitions()) {
					// partitionMode
					partitionMode = partition.getMode();
					// size
					size += partition.getRecordNum();
					// tables , dicts
					String tname = partition.getTname();
					tables.add(tname);
					models.add(tname.split(MetaConstants.META_DELIM_NAME)[0]);
					for(RDict dict: partition.getDicts()){
						dicts.add(dict.getTable());
					}
					// columns
					for(RColumn c: partition.getCols()) columns.add(c.getName());
					for(RMetric m: partition.getMs()) columns.add(m.getName());
					for(RField f: partition.getCondition().getColumns())columns.add(f.getName());
				}
			}
		}
	}
}

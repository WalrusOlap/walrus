package org.pcg.walrus.core.plan.operator;

import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.query.RPartition;

public class Selector implements Operator {

	private static final long serialVersionUID = -3163145305948257184L;

	private List<RPartition> partitions; // instance of queries

	private Date bday;
	private Date eday;

	public Selector(Date bday, Date eday) {
		this.bday = bday;
		this.eday = eday;
		partitions = Lists.newArrayList();
	}

	public void addPartition(RPartition partition){
		partitions.add(partition);
	}
	
	public void setPartitions(List<RPartition> partitions){
		this.partitions = partitions;
	}

	public List<RPartition> getPartitions() {
		return partitions;
	}

	public Date getBday() {
		return bday;
	}

	public Date getEday() {
		return eday;
	}

	@Override
	public String getOperatorType() {
		return CoreConstants.AST_OPER_SELECT;
	}
	
	@Override
	public String toString() {
		String ps = "";
		for(RPartition p: partitions) ps += p.toString();
		return "Selector: " + ps;
	}
}

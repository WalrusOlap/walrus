package org.pcg.walrus.core.plan.operator;

import java.util.Date;

import org.apache.commons.lang.ArrayUtils;
import org.pcg.walrus.core.CoreConstants;

/**
 * query final aggregator
 */
public class Aggregator extends Combiner {

	private static final long serialVersionUID = -7806760000857611642L;

	private String orderBy;
	private int limit;

	public Aggregator(Date bday, Date eday) {
		super(bday, eday);
	}

	public String getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	@Override
	public String getOperatorType() {
		return CoreConstants.AST_OPER_AGGREGATOR;
	}

	@Override
	public String toString() {
		return "Aggregator: {cols: " + ArrayUtils.toString(cols) + "}{MS: " + ArrayUtils.toString(ms) + "}";
	}
}

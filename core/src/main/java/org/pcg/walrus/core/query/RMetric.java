package org.pcg.walrus.core.query;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.parse.aggregations.metrics.MetricFunction;
import org.pcg.walrus.meta.pb.WFieldMessage;

import java.util.ArrayList;
import java.util.List;

/**
* Class <code>RMetric</code>
* walrus query metric
*/
public class RMetric extends RField {
	
	private static final long serialVersionUID = -4854734560903358319L;

	private WFieldMessage.WMetric metric;
	private WFieldMessage.WMetric joinMetric; // join metric
	private MetricFunction aggregateFuntion = null;
	private String method;

	public RMetric(String name){
		super(name);
	}
	
	public RMetric(String name, String type){
		super(name, type);
	}

	public RMetric(WFieldMessage.WMetric metric) {
		super(metric.getName(), metric.getType());
		this.metric = metric;
	}

	// add metric
	public static RMetric[] addMetric(RMetric[] cols, RMetric col) {
		List<RMetric> rs = new ArrayList<>();
		boolean contain = false;
		for (RMetric f : cols) {
			if(f.getName().equals(col.getName())) {
				contain = true;
				rs.add(col);
			}
			else rs.add(f);
		}
		if(!contain) rs.add(col);
		return rs.toArray(new RMetric[rs.size()]);
	}

	// add metric
	public static RMetric[] removeMetric(RMetric[] cols, RMetric col) {
		if(!contains(cols, col)) return cols;
		List<RMetric> rs = new ArrayList<>();
		for (RMetric f : cols)
			if(!f.getName().equals(col.getName())) rs.add(f);
		return rs.toArray(new RMetric[rs.size()]);
	}
	// convert String to metrics
	public static RMetric[] toMetrics(String[] ms) {
		RMetric[] columns = new RMetric[ms.length];
		int index = 0;
		for (String metric : ms) columns[index++] = new RMetric(metric);
		return columns;
	}

	// column derive mode
	public String getDerivedMode() {
		if(exType != null) return exType;
		else if(metric != null) return metric.getDerivedMode();
		else return CoreConstants.DERIVE_MODE_SELECT;
	}

	@Override
	public List<String> getParamList() {
		if(metric != null && metric.getDerivedLogic() != null)
			return metric.getDerivedLogic().getParamsList();
		else return null;
	}

	@Override
	public RMetric copy() {
		RMetric m = new RMetric(getName(), getType());
		m.setMetric(metric);
		m.setAlias(alias);
		m.setRefer(refer);
		m.setExType(exType);
		m.setMethod(method);
		m.setParam(param);
		m.setJoinMetric(joinMetric);
		return m;
	}

	public WFieldMessage.WMetric getMetric() {
		return metric;
	}
	
	public void setMetric(WFieldMessage.WMetric metric) {
		this.metric = metric;
	}

	public WFieldMessage.WMetric getJoinMetric() {
		return joinMetric;
	}

	public void setJoinMetric(WFieldMessage.WMetric joinMetric) {
		this.joinMetric = joinMetric;
	}

	public String getMethod() {
		if(method != null) return method;
		else if(metric != null) return metric.getMethod();
		else return null;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public MetricFunction getAggregateFuntion() {
		return aggregateFuntion;
	}

	public void setAggregateFuntion(MetricFunction function) {
		this.aggregateFuntion = function;
	}

	@Override
	public String toString() {
		return getName() + "[" + getDerivedMode() + "," + getMethod() + "," + getParam() + "]";
	}
}

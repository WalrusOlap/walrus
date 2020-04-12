package org.pcg.walrus.core.query;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.Sets;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.common.util.TimeUtil;

/**
 * Class <code>RQuery</code> walrus query
 */
public class RQuery implements Serializable {

	private static final long serialVersionUID = -5615757971597374246L;

	private String level;
	private String tname;

	private Date bday;
	private Date eday;

	private Set<String> tables = new HashSet<String>();
	private RColumn[] cols;
	private RMetric[] ms;
	private RCondition condition;

	private String orderBy;
	private int limit = 0;

	private Map<String, Object> params;

	public RQuery(String tname, Date bday, Date eday) {
		this.tname = tname;
		this.bday = bday;
		this.eday = eday;
	}

	/**
	 * split query based on split range <br>
	 * e.g. dayRange = 0 <br>
	 * then. [20170301-20170315] => [20170301-20170314] + [20170315-20170315] <br>
	 * then. [20170315-20170315] => [null] + [20170315-20170315] <br>
	 * @throws ParseException 
	 */
	public RQuery[] split(int dayRange) throws WCoreException {
		RQuery[] querys = new RQuery[2];
		int[] splits = TimeUtil.splitDays(bday, eday, dayRange);
		if(bday.after(eday)) {
			querys[0] = null;
			querys[1] = copy(bday, bday);
		} else {
			try {
				querys[0] = copy(bday, TimeUtil.intToDate(splits[1]));
				querys[1] = copy(TimeUtil.intToDate(splits[2]), eday);
			} catch (ParseException e) {
				throw new WCoreException(e.getMessage());
			}
		}
		return querys;
	}

	/**
	 * @return all columns in group by , filter , metric
	 */
	public Set<String> fields() {
		Set<String> columns = Sets.newHashSet();
		for (RColumn col : cols)
			if (!CoreConstants.DERIVE_MODE_VIRTUAL.equals(col.getDerivedMode()))
				columns.add(col.getName());
		for (RMetric m : ms)
			if (!CoreConstants.DERIVE_MODE_VIRTUAL.equals(m.getDerivedMode()))
				columns.add(m.getName());
		for (RField field: condition.getColumns())
			if (!CoreConstants.DERIVE_MODE_VIRTUAL.equals(field.getDerivedMode()))
				columns.add(field.getName());
		return columns;
	}

	/**
	 * @return all columns in group by , filter
	 */
	public Set<String> realColumns() {
		Set<String> columns = Sets.newHashSet();
		for (RColumn col : cols)
			if (!CoreConstants.DERIVE_MODE_VIRTUAL.equals(col.getDerivedMode()))
				columns.add(col.getName());
		for (RField field: condition.getColumns())
			columns.add(field.getName());
		return columns;
	}
	
	/**
	 * @return all columns in metric
	 */
	public Set<String> realMetrics() {
		Set<String> columns = Sets.newHashSet();
		for (RMetric col : ms)
			// ignore virtual|extension metric
			if (CoreConstants.DERIVE_MODE_VIRTUAL.equals(col.getDerivedMode())
				|| CoreConstants.DERIVE_MODE_EXTEND.equalsIgnoreCase(col.getDerivedMode())) continue;
			else columns.add(col.getName());
		return columns;
	}
	
	/**
	 * a new query with bday, eday <br>
	 * !be careful, this is not a full deep copy!
	 */
	public RQuery copy(Date bday, Date eday) {
		RQuery query = new RQuery(tname, bday, eday);
		query.setLevel(level);
		RColumn[] columns = new RColumn[cols.length];
		int index = 0;
		for(RColumn col: cols) columns[index++] = col.copy();
		query.setCols(columns);
		query.setCondition(condition);
		RMetric[] metrics = new RMetric[ms.length];
		index = 0;
		for(RMetric metric: ms) metrics[index++] = metric.copy();
		query.setMs(metrics);
		query.setParams(params);
		return query;
	}

	/**
	 * set metric to virtual
	 */
	public void setVirtual(Set<String> columns) {
		for (RColumn col : cols) {
			if (columns.contains(col.getName()))
				col.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
		}
		for (RMetric metric : ms) {
			if (columns.contains(metric.getName()))
				metric.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
		}
		for (RField field: condition.getColumns()) {
			if (columns.contains(field.getName()))
				field.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
		}
	}

	/**
	 * add column to columns
	 */
	public void addColumn(String colName) {
		this.cols = RColumn.addColumn(cols, new RColumn(colName));
	}

	/**
	 * add column to columns
	 */
	public void addColumn(RColumn col) {
		this.cols = RColumn.addColumn(cols, col);
	}

	/**
	 * remove column from cols
	 */
	public void removeColumn(String colName) {
		this.cols = RColumn.removeColumn(cols, new RColumn(colName));
	}
	
	/**
	 * add metric to ms
	 */
	public void addMetric(String metric) {
		addMetric(new RMetric(metric));
	}
	
	/**
	 * add metric to ms
	 */
	public void addMetric(RMetric metric) {
		this.ms = RMetric.addMetric(ms, metric);
	}

	/**
	 * remove metric from ms
	 */
	public void removeMetric(String metric) {
		this.ms = RMetric.removeMetric(ms, new RMetric(metric));
	}

	/**
	 * if query contains field
	 */
	public boolean queryField(String field) {
		return fields().contains(field);
	}

	public void setLevel(String cluster) {
		this.level = cluster;
	}

	public String getLevel() {
		return level;
	}

	public RColumn[] getCols() {
		return cols;
	}

	public void setCols(RColumn[] cols) {
		this.cols = cols;
	}

	public RMetric[] getMs() {
		return ms;
	}

	public void setMs(RMetric[] ms) {
		this.ms = ms;
	}

	public void setCondition(RCondition condition) {
		this.condition = condition;
	}

	public RCondition getCondition() {
		return condition;
	}

	public String getTname() {
		return tname;
	}

	public Date getBday() {
		return bday;
	}

	public Date getEday() {
		return eday;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
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
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(tname + "[" + TimeUtil.DateToString(bday, "yyyy-MM-dd HH:mm:ss") + "-" + TimeUtil.DateToString(eday, "yyyy-MM-dd HH:mm:ss") + "](" + level + ")");
		sb.append("\n\tcols: " + ArrayUtils.toString(cols));
		sb.append("\n\tms: " + ArrayUtils.toString(ms));
		sb.append("\n\tcondition: " + condition);
		return sb.toString();
	}
	
	public void addTable(String table) {
		tables.add(table);
	}
	
	public Set<String> getTables() {
		return tables;
	}
}

package org.pcg.walrus.core.query;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

/**
 * RPartition</br>
 * 	the deepest query for each partition, contains all information for a execute script, e.g sql,pig,druid...
 */
public class RPartition implements Serializable {

	private static final long serialVersionUID = -6487105388382833861L;

	private String mode;
	private String partition;
	private String path;
	private String format;
	private String schema;

	private String tname;
	
	private Date bday;
	private Date eday;
	
	private RColumn[] cols;
	private RMetric[] ms;
	private RCondition condition;
	
	private Set<RDict> dicts;

	private long recordNum;

	public RPartition(String mode, Date bday, Date eday, String partition) {
		this.bday = bday;
		this.eday = eday;
		this.partition = partition;
		this.mode = mode;
	}
	
	/**
	 * deep copy
	 */
	public RPartition copy() {
		RPartition p = new RPartition(mode, bday, eday, partition);
		RColumn[] columns = new RColumn[cols.length];
		int index = 0;
		for(RColumn col: cols) columns[index++] = col.copy();
		RMetric[] metrics = new RMetric[ms.length];
		index = 0;
		for(RMetric metric: ms) metrics[index++] = metric.copy();
		// copy condition 
		p.setCondition(condition.copy());
		p.setMs(metrics);
		p.setCols(columns);
		// copy dicts
		Set<RDict> ds = new HashSet<>();
		for(RDict dict: dicts) ds.add(dict.copy());
		p.setDicts(ds);
		p.setFormat(format);
		p.setPath(path);
		p.setSchema(schema);
		p.setTname(tname);
		return p;
	}

	// update dict join table
	public void updateDictTable(String dict, String table) {
		for(RDict d: dicts)
			if(d.getDictName().equalsIgnoreCase(dict)) d.setTable(table);
	}

	public void setBday(Date bday) {
		this.bday = bday;
	}

	public void setEday(Date eday) {
		this.eday = eday;
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

	public RCondition getCondition() {
		return condition;
	}

	public void setCondition(RCondition condition) {
		this.condition = condition;
	}

	public Date getBday() {
		return bday;
	}

	public Date getEday() {
		return eday;
	}

	public String getPartition() {
		return partition;
	}
	
	public String getMode() {
		return mode;
	}

	public String getTname() {
		return tname;
	}

	public void setTname(String tname) {
		this.tname = tname;
	}

	public Set<RDict> getDicts() {
		return dicts;
	}

	public void setDicts(Set<RDict> dicts) {
		this.dicts = dicts;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	@Override
	public String toString() {
		return "(" + tname + "[" + bday + "-" + eday
				+ "]cols: " + ArrayUtils.toString(cols)
				+ ", ms: " + ArrayUtils.toString(ms)
				+ ", dicts: " + dicts 
				+ ", condition: " + condition 
				+ ")";
	}
	
	@Override
	public boolean equals(Object obj) {
        if (obj instanceof RPartition) {   
        	RPartition r = (RPartition) obj;   
            return partition.equalsIgnoreCase(r.getPartition());   
        }
        return super.equals(obj);
	}
	
	@Override
	public int hashCode() {
		return partition.hashCode();
	}

	public long getRecordNum() {
		return recordNum;
	}

	public void setRecordNum(long recordNum) {
		this.recordNum = recordNum;
	}

}

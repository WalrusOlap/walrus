package org.pcg.walrus.meta.partition;

import java.util.Date;

/**
 * Shark partition:
 *   1 only partitioned by time range is supported
 *   2 partitioned by single|multiple column is not supported:
 *   	most data store(e.g, parquet, hive) support partitioned by column, so we just split job by time range and push down filter to data store
 */
public class SPartition {

	private String name;
	private Date bday;
	private Date eday;
	
	public SPartition(Date bday, Date eday) {
		this.bday = bday;
		this.eday = eday;		
	}
	
	public SPartition(Date bday, Date eday, String name) {
		this(bday, eday);
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getBday() {
		return bday;
	}

	public Date getEday() {
		return eday;
	}
	
	@Override
	public String toString() {
		return name + "[" + bday + "-" + eday + "]";
	}
}

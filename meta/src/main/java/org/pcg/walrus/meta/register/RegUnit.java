package org.pcg.walrus.meta.register;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.common.util.EncryptionUtil;

/**
 * RegUnit contains all information for registration
 */
public class RegUnit implements Comparable<RegUnit> {

	private String name;
	// meta
	private WTableMessage.WPartitionTable partition;
	private WTableMessage.WBaseTable table;
	private WTableMessage.WTablet tablet;
	// status
	private volatile int status;
	// registered data
	private Dataset<Row> registData;

	public RegUnit(String name, WTableMessage.WPartitionTable partition, WTableMessage.WBaseTable table, WTableMessage.WTablet tablet) {
		this.name = name;
		this.table = table;
		this.partition = partition;
		this.tablet = tablet;
	}
	
	/**
	 * MD5(columns) on this table
	 * to mark if this table changed
	 */
	public String getSignature() {
		Set<String> columns = new HashSet<String>();
		for(WFieldMessage.WDimension dim: partition.getDimensionsList()) columns.add(dim.getName());
		for(WFieldMessage.WMetric m: partition.getMetricsList()) columns.add(m.getName());
		return EncryptionUtil.md5(columns.toString());
	}

	public WTableMessage.WBaseTable getTable() {
		return table;
	}

	public WTableMessage.WPartitionTable getPartition() {
		return partition;
	}

	public String getName() {
		return name;
	}

	public WTableMessage.WTablet getTablet() {
		return tablet;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public Dataset<Row> getRegistData() {
		return registData;
	}

	public void setRegistData(Dataset<Row> registData) {
		this.registData = registData;
	}

	// TODO
	public int getPriority() {
		return 0;
	}

	@Override
	public String toString() {
		return "name: " + name + ", status: " + status;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (obj instanceof RegUnit) {
        	RegUnit r = (RegUnit) obj;
            return name.equalsIgnoreCase(r.getName());   
        }
        return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public int compareTo(RegUnit r) {
		return r.getPriority() - getPriority();
	}

}

package org.pcg.walrus.meta.tree;

import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.partition.SPartition;
import org.pcg.walrus.meta.partition.PartitionSplitor;
import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.common.util.TimeUtil;

/**
 * MTableNode
 */
public class MTableNode {

	private String name;
	private WTableMessage.WTable table;

	private Map<String, MPartitionNode> partitions;

	public MTableNode(String name, WTableMessage.WTable table) {
		this.name = name;
		this.table = table;
		partitions = Maps.newConcurrentMap();
	}
	
	public String getName() {
		return name;
	}

	public WTableMessage.WTable getTable() {
		return table;
	}

	public Map<String, MPartitionNode> getPartitions() {
		return partitions;
	}

	/**
	 * find all cover days
	 */
	public Set<String> coverDays(String level) {
		Set<String> days = Sets.newHashSet();
		// if not this cluster, return empty
		if(!MetaConstants.CLUSTER_LEVEL_ALL.equalsIgnoreCase(level) && !table.getLevelList().contains(level)) return days;
		for(MPartitionNode partition: partitions.values()) {
			try {
				WTableMessage.WBaseTable table = partition.getBaseTableNode().getTable();
				Date bday = TimeUtil.stringToDate(table.getStartTime());
				Date eday = TimeUtil.stringToDate(table.getEndTime());
				Date[] ds = TimeUtil.getPartitonDays(bday, eday);
				for(Date d: ds) days.add(TimeUtil.DateToString(d));
				Set<Integer> us = TimeUtil.unsupportedDays(table.getValidDay(), bday, eday);
				for(Integer d: us) days.remove(String.valueOf(d));
			} catch (ParseException e) {
				// do nothing
				e.printStackTrace();
			}
			
		}
		return days;
	}

	/**
	 * find all columns this table
	 */
	public Set<String> columns(Date bday, Date eday, String level, boolean isIntersection) {
		Set<String> columns = new HashSet<>();
		columns.addAll(dims(bday, eday, level, isIntersection).keySet());
		columns.addAll(metrics(bday, eday, level, isIntersection).keySet());
		return columns;
	}
	
	/**
	 * find all dims this table
	 */
	public Map<String, WFieldMessage.WDimension> dims(Date bday, Date eday, String level, boolean isIntersection) {
		Map<String, WFieldMessage.WDimension> cols = new HashMap<>();
		scanFields(bday, eday, MetaConstants.META_FIELD_DIM, level, isIntersection, null, cols);
		return cols;
	}
	
	/**
	 * find all metrics this table
	 */
	public Map<String, WFieldMessage.WMetric> metrics(Date bday, Date eday, String level, boolean isIntersection) {
		Map<String, WFieldMessage.WMetric> metrics = new HashMap<>();
		scanFields(bday, eday, MetaConstants.META_FIELD_METRIC, level, isIntersection, metrics, null);
		return metrics;
	}

	// return columns exist in all partitions
	private void scanFields(Date bday, Date eday, String field, String level, boolean isIntersection,
			Map<String, WFieldMessage.WMetric> metrics, Map<String, WFieldMessage.WDimension> cols) {
		// if not this cluster, return empty
		if(!MetaConstants.CLUSTER_LEVEL_ALL.equalsIgnoreCase(level)
				&& !table.getLevelList().contains(level)) return;
		List<SPartition> splits = PartitionSplitor.splits(table.getPartitionMode(), bday, eday);
		for(SPartition split: splits) {
			String partionName = split.getName();
			if(!partitions.containsKey(partionName)) continue;
			WTableMessage.WPartitionTable partition = partitions.get(partionName).getPartition();
			List<WFieldMessage.WDimension> dims = partition.getDimensionsList();
			List<WFieldMessage.WMetric> ms = partition.getMetricsList();
			switch (field) {
			case MetaConstants.META_FIELD_METRIC:
				for(WFieldMessage.WMetric m: ms) metrics.put(m.getName(), m);
				break;
			case MetaConstants.META_FIELD_DIM:
				for(WFieldMessage.WDimension dim: dims) cols.put(dim.getName(), dim);
				break;
			default:
				break;
			}
		}
	}

	/**
	 * table clusters
	 */
	public List<String> levels(){
		return table.getLevelList();
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		// add first 5 partition
		Iterator<Entry<String, MPartitionNode>> it = partitions.entrySet().iterator();
		int index = 0;
		while(it.hasNext() && index++ < 3) 
			sb.append("\n\t\t\t" + partitions.get(it.next().getKey()));
		if(index > 2) sb.append("\n\t\t\t... ...");
		return sb.toString();
	}

}

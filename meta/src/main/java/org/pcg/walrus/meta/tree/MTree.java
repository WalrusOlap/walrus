package org.pcg.walrus.meta.tree;

import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.register.MetaRegister;
import org.pcg.walrus.meta.register.RegUnit;

/**
 * META:
 * 	 view(@class <code>MViewNode<code>)
 * 		joins
 *   		table_1
 *   		table_2
 * 		unions
 * 			table_1
 * 				roll_up_1
 * 				roll_up_2
 * 			view_2
 * 				joins
 * 				unions
 *   table(@class <code>MTableNode<code>)
 *   	table_1
 *   		partition_1(@class <code>MPartitionNode<code>)
 *   			base
 *   				tablet_1(@class <code>MTabletNode<code>)
 *   				tablet_2
 *   		partition_2
 *   			...
 *   	...
 *   bc_table(@class <code>MTableNode<code>)
 *   	table_1
 *   	table_2
 *
 */
public class MTree {
	
	private Map<String, MViewNode> viewTree;
	private Map<String, MTableNode> tableTree;
	private Map<String, MTableNode> dictTree;

	// tablets to regist
	private List<RegUnit> containers;
	
	public MTree() {
		viewTree = Maps.newConcurrentMap();
		tableTree = Maps.newConcurrentMap();
		dictTree = Maps.newConcurrentMap();
		containers = Lists.newArrayList();
	}
	
	public Map<String, MViewNode> getViewTree() {
		return viewTree;
	}

	public Map<String, MTableNode> getTableTree() {
		return tableTree;
	}

	public Map<String, MTableNode> getDictTree() {
		return dictTree;
	}

	/**
	 * union two tree
	 */
	public void union(MTree tree){
		viewTree.putAll(tree.getViewTree());
		tableTree.putAll(tree.getTableTree());
		dictTree.putAll(tree.getDictTree());
	}
	
	/**
	 * if table readay on day
	 * @param table
	 * @param day
	 * @throws ParseException 
	 */
	public boolean isTableReady(String cluster, String table, int day) throws ParseException {
		return isTableReady(cluster, table, day, -1);
	}
	
	/**
	 * if table readay on day, hour
	 * @param table
	 * @param hour
	 * @throws ParseException 
	 */
	public boolean isTableReady(String cluster, String table, int day, int hour) throws ParseException {
		if(tableTree.containsKey(table)) return isReady(tableTree.get(table), cluster, day, hour);
		else if (dictTree.containsKey(table)) return isReady(dictTree.get(table), cluster, day, hour);
		else return false;
	}

	// check table ready
	private boolean isReady(MTableNode table, String cluster, int day, int hour) throws ParseException {
		boolean ready = Boolean.FALSE;
		String pMode = table.getTable().getPartitionMode();
		switch (pMode) {
		case MetaConstants.PARTITION_MODE_H:
			String partition = day + "_" + hour;
			ready = table.getPartitions().containsKey(partition);
			break;
		case MetaConstants.PARTITION_MODE_A:
		case MetaConstants.PARTITION_MODE_Y:
		case MetaConstants.PARTITION_MODE_M:
		case MetaConstants.PARTITION_MODE_D:
		default:
			Set<String> days = table.coverDays(cluster);
			ready = days.contains(String.valueOf(day));
			break;
		}
		return ready;
	}

	/**
	 * get all tablets for registration
	 */
	public List<RegUnit> getRContainers() {
		containers.clear();
		// add table tree
		addContainer(tableTree.values());
		addContainer(dictTree.values());
		return containers;
	}
	
	private void addContainer(Collection<MTableNode> tables) {
		for(MTableNode table: tables) {
			for(MPartitionNode partition: table.getPartitions().values()) {
				MBaseTableNode base = partition.getBaseTableNode();
				MTabletNode tablet = base.getTabletNode();
				// create container
				RegUnit container = new RegUnit(tablet.getName(),
						partition.getPartition(), base.getTable(), tablet.getTablet());
				container.setStatus(MetaRegister.R_STATUS_INIT);
				containers.add(container);
			}
		}
	}
	
	/**
	 * deep copy a meta tree
	 */
	public MTree copy() {
		MTree tree = new MTree();
		tree.getViewTree().putAll(viewTree);
		tree.getTableTree().putAll(tableTree);
		tree.getDictTree().putAll(dictTree);
		return tree;
	}

	/**
	 * Meta Tree:
	 * 		Views:
	 *		Tables:
	 *		BCTables:
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("Meta Tree:\n");
		sb.append("\tViews:\n");
		for(String view: viewTree.keySet()) {
			sb.append("\t\t" + viewTree.get(view) + "\n");
		}
		sb.append("\tTables:\n");
		for(String table: tableTree.keySet()) {
			sb.append("\t\t" + table + ": " + tableTree.get(table) + "\n");
		}
		sb.append("\tDicts:\n");
		for(String table: dictTree.keySet()) {
			sb.append("\t\t" + table + ": " + dictTree.get(table) + "\n");
		}
		return sb.toString();
	}
}

package org.pcg.walrus.meta.tree;

import org.pcg.walrus.meta.pb.WTableMessage;

public class MBaseTableNode {

	private String name;
	private WTableMessage.WBaseTable table;
	private MTabletNode tabletNode;

	public MBaseTableNode(String name, WTableMessage.WBaseTable table, MTabletNode tabletNode) {
		this.name = name;
		this.table = table;
		this.tabletNode = tabletNode;
	}

	public String getName() {
		return name;
	}

	public WTableMessage.WBaseTable getTable() {
		return table;
	}

	public MTabletNode getTabletNode() {
		return tabletNode;
	}
	
	@Override
	public String toString() {
		return table.getStartTime() + "-" + table.getEndTime() + ", " + tabletNode;
	}

}

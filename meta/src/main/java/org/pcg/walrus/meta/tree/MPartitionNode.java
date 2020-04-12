package org.pcg.walrus.meta.tree;

import org.pcg.walrus.meta.pb.WTableMessage;

/**
 * MPartitionNode
 */
public class MPartitionNode {

	private String name;
	private WTableMessage.WPartitionTable partition;
	private MBaseTableNode baseTableNode;

	public MPartitionNode(String name, WTableMessage.WPartitionTable partition,
			MBaseTableNode baseTableNode) {
		this.name = name;
		this.partition = partition;
		this.baseTableNode = baseTableNode;
	}

	public String getName() {
		return name;
	}

	public WTableMessage.WPartitionTable getPartition() {
		return partition;
	}

	public MBaseTableNode getBaseTableNode() {
		return baseTableNode;
	}
	
	@Override
	public String toString() {
		return partition.getPartitionKey() + ": [" + baseTableNode + "]";
	}
}

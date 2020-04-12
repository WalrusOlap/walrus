package org.pcg.walrus.meta.tree;

/**
 * MViewNode
 */
public class MViewTableNode {

	private String name;
	private int level;

	public MViewTableNode(String name, int level) {
		this.name = name;
		this.level = level;
	}

	public int getLevel() {
		return level;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return name + ": " + level;
	}

}

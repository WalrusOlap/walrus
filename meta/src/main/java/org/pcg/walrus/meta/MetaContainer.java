package org.pcg.walrus.meta;

import org.pcg.walrus.meta.tree.MTree;

/**
 * MetaContainer double buffer container for <code>MTree<code>
 * 	safely switch tree when flush meta
 */
public class MetaContainer {

	private MTree[] trees;
	private volatile int currentIndex = 1;

	public MetaContainer() {
		trees = new MTree[2];
	}

	public boolean update(MTree tree) {
		if (currentIndex != 0 && currentIndex != 1) {
			return false;
		}
		int reloadIndex = 1 - currentIndex;
		trees[reloadIndex] = tree;
		currentIndex = reloadIndex;
		return true;
	}
	
	/**
	 * @return current tree
	 */
	public MTree currentTree() {
		return trees[currentIndex];
	}
	
	/**
	 * switch to the other tree
	 */
	public void switchTree() {
		if (currentIndex != 0 && currentIndex != 1) return;
		currentIndex = 1 - currentIndex;
	}
	
	/**
	 * copy a current tree
	 */
	public MTree copyTree() {
		return currentTree().copy();
	}
	
	/**
	 * switch to the other tree
	 */
	public boolean isLoaded() {
		return currentTree() != null;
	}
}

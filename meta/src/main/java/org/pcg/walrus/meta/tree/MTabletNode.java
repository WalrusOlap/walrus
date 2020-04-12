package org.pcg.walrus.meta.tree;

import org.pcg.walrus.meta.pb.WTableMessage;

public class MTabletNode {

	private String name;
	private WTableMessage.WTablet tablet;

	public MTabletNode(String name, WTableMessage.WTablet tablet) {
		this.name = name;
		this.tablet = tablet;
	}

	public String getName() {
		return name;
	}

	public WTableMessage.WTablet getTablet() {
		return tablet;
	}
	
	@Override
	public String toString() {
		return name + ", format: " + tablet.getFormat() + ", path: " + tablet.getPath();
	}
}

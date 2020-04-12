package org.pcg.walrus.meta.tree;

import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WViewMessage;

import java.util.ArrayList;
import java.util.List;

public class MViewNode {

	private String name;
	private WViewMessage.WView view;
	
	public MViewNode(String name, WViewMessage.WView view) {
		this.name = name;
		this.view = view;
	}

	public WViewMessage.WView getView() {
		return view;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return scanView(view);
	}
	
	private String scanView(WViewMessage.WView view){
		List<String> joins = new ArrayList<String>();
		for(WFieldMessage.WDict dict: view.getJoinsList()) joins.add(dict.getDictName());
		List<String> cubes = new ArrayList<String>();
		for(WViewMessage.WCube cube: view.getCubesList()) cubes.add(cube.getName());
		List<String> views = new ArrayList<String>();
		for(WViewMessage.WView v: view.getUnionsList()) views.add(scanView(v));
		String viewStr = view.getName() + "[" + view.getUnionMode() + "]";
		if(joins.size() > 0) viewStr = viewStr + ", joins:" + joins;
		if(cubes.size() > 0) viewStr = viewStr + ", cubes:" + cubes;
		if(views.size() > 0) viewStr = viewStr + ", unions:" + views;
		return viewStr;
	}

}

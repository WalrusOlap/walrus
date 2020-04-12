package org.pcg.walrus.meta.register;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.collect.Maps;
import com.googlecode.protobuf.format.JsonFormat;
import com.googlecode.protobuf.format.JsonFormat.ParseException;

import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.meta.pb.WViewMessage;
import org.pcg.walrus.meta.tree.MBaseTableNode;
import org.pcg.walrus.meta.tree.MPartitionNode;
import org.pcg.walrus.meta.tree.MTableNode;
import org.pcg.walrus.meta.tree.MTabletNode;
import org.pcg.walrus.meta.tree.MTree;
import org.pcg.walrus.meta.tree.MViewNode;

import org.pcg.walrus.common.io.ZkClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaLoader load meta on zk: /walrus/meta/view|table|dict|job
 */
public class MetaLoader implements Serializable {

	private static final long serialVersionUID = -7742462364582760341L;

	private static final Logger log = LoggerFactory.getLogger(MetaLoader.class);

	private ZkClient client;
	private String level;

	public MetaLoader(ZkClient client, String level) {
		this.client = client;
		this.level = level;
	}

	/**
	 * create all meta path if not exist
	 */
	public void ensureMetaPath() throws Exception {
		String[] paths = new String[]{
				MetaConstants.ZK_PATH_META_VIEW,
				MetaConstants.ZK_PATH_META_TABLE,
				MetaConstants.ZK_PATH_META_DICT,
		};
		for(String path: paths) client.setData(path, "".getBytes());
	}

	/**
	 * load view|table|bc_table node
	 */
	public MTree loadAll() throws Exception {
		MTree tree = new MTree();
		// load view
		List<String> views = client.getChildren(MetaConstants.ZK_PATH_META_VIEW);
		log.info("meta views: " + views);
		for (String view : views) {
			String viewPath = MetaConstants.ZK_PATH_META_VIEW + "/" + view;
			MViewNode viewNode = loadViewNode(viewPath);
			if(viewNode != null) tree.getViewTree().put(view, viewNode);
		}
		Date bday = null;
		Date eday = null;
		try {
			bday = TimeUtil.intToDate(MetaConstants.DEFAULT_BDAY);
			eday = TimeUtil.intToDate(MetaConstants.DEFAULT_EDAY);
		} catch (java.text.ParseException e) {
			// do nothing
		}
		// load table
		List<String> tables = client.getChildren(MetaConstants.ZK_PATH_META_TABLE);
		log.info("meta tables: " + tables);
		for (String table : tables) {
			String tablePath = MetaConstants.ZK_PATH_META_TABLE + "/" + table;
			MTableNode tableNode = loadTableNode(tablePath, bday, eday);
			if(tableNode != null) tree.getTableTree().put(table, tableNode);
		}
		// load dict
		List<String> dicts = client.getChildren(MetaConstants.ZK_PATH_META_DICT);
		log.info("meta dicts: " + dicts);
		for (String dict : dicts) {
			String dictPath = MetaConstants.ZK_PATH_META_DICT + "/" + dict;
			MTableNode tableNode = loadTableNode(dictPath, bday, eday);
			if(tableNode != null) tree.getDictTree().put(dict, tableNode);
		}
		return tree;
	}

	// if view, load view and all tables in this view
	// if table, just load table
	public MTree loadView(String viewName, Date bday, Date eday) {
		MTree tree = new MTree();
		String viewPath = MetaConstants.ZK_PATH_META_VIEW + "/" + viewName;
		String tablePath = MetaConstants.ZK_PATH_META_TABLE + "/" + viewName;
		if(client.exists(viewPath)) {
			MViewNode viewNode = loadViewNode(viewPath);
			WViewMessage.WView view = viewNode.getView();
			tree.getViewTree().put(viewName, viewNode);
			tree.union(traverseView(view, bday, eday));
		} else if (client.exists(tablePath)) {
			tree = loadTable(viewName, bday, eday);
		}
		return tree;
	}

	// load table
	public MTree loadTable(String tableName, Date bday, Date eday) {
		MTree tree = new MTree();
		String tablePath = MetaConstants.ZK_PATH_META_TABLE + "/" + tableName;
		MTableNode table = loadTableNode(tablePath, bday, eday);
		if(table != null)
			tree.getTableTree().put(tableName, table);
		return tree;
	}

	// update tree
	public void updateTree(String oper, MTree tree, String type, String name, String partition) throws Exception {
		log.info("update meta[" + type + "->" + name + "]: " + partition);
		Date bday = TimeUtil.defaultBday();
		Date eday = TimeUtil.defaultEday();
		switch (oper) {
			case "delete":
				switch (type) {
					case "view":
						tree.getViewTree().remove(name);
						break;
					case "table":
						tree.getTableTree().remove(name);
						break;
					case "dict":
						tree.getDictTree().remove(name);
						break;
					default:
						break;
				}
				break;
			case "add":
				switch (type) {
					case "view":
						tree.getViewTree().put(name, loadViewNode(MetaConstants.ZK_PATH_META_VIEW + "/" + name));
						break;
					case "table":
						MTableNode node = loadTableNode(MetaConstants.ZK_PATH_META_TABLE + "/" + name, bday, eday);
						if (node != null) tree.getTableTree().put(name, node);
						break;
					case "dict":
						MTableNode dictNode = loadTableNode(MetaConstants.ZK_PATH_META_DICT + "/" + name, bday, eday);
						if (dictNode != null) tree.getDictTree().put(name, dictNode);
						break;
					default:
						break;
				}
				break;
			case "update":
			default:
				switch (type) {
					case "view":
						tree.getViewTree().put(name, loadViewNode(MetaConstants.ZK_PATH_META_VIEW + "/" + name));
						break;
					case "table":
						if (tree.getTableTree().containsKey(name)) {
							String tablePath = MetaConstants.ZK_PATH_META_TABLE + "/" + name + "/" + partition;
							MPartitionNode node = loadPartitionNode(name, tablePath);
							tree.getTableTree().get(name).getPartitions().put(partition, node);
						}
						break;
					case "dict":
						if (tree.getDictTree().containsKey(name)) {
							String dictPath = MetaConstants.ZK_PATH_META_DICT + "/" + name + "/" + partition;
							MPartitionNode dictNode = loadPartitionNode(name, dictPath);
							tree.getDictTree().get(name).getPartitions().put(partition, dictNode);
						}
						break;
					default:
						break;
				}
				break;
		}
	}

	/**
	 * traverse a View node and return a new tree
	 */
	private MTree traverseView(WViewMessage.WView view, Date bday, Date eday) {
		MTree tree = new MTree();
		// load joins
		if(view.getJoinsList() != null)
			for(WFieldMessage.WDict d: view.getJoinsList()) loadDict(d, tree, bday, eday);
		// if leaf, load table and cubes, else load view
		if(view.getUnionsCount() == 0) {
			String viewPath = MetaConstants.ZK_PATH_META_VIEW + "/" + view.getName();
			String tablePath = MetaConstants.ZK_PATH_META_TABLE + "/" + view.getName();
			if(client.exists(tablePath)) {
				// load base table
				MTableNode leaf = loadTableNode(tablePath, bday, eday);
				if(leaf != null)
					tree.getTableTree().put(leaf.getName(), leaf);
				// load rollups
				if(view.getCubesList() != null) {
					for(WViewMessage.WCube r: view.getCubesList()) {
						// traverse rollups
						tree.union(loadCube(r, bday, eday));
					}
				}
			} else if (client.exists(viewPath)){
				MViewNode vNode = loadViewNode(viewPath);
				tree.getViewTree().put(vNode.getName(), vNode);
				tree.union(traverseView(vNode.getView(), bday, eday));
			}
		} else {
			for(WViewMessage.WView v: view.getUnionsList()) {
				tree.union(traverseView(v, bday, eday));
			}
		}
		return tree;
	}

	/**
	 * traverse dict
	 */
	private void loadDict(WFieldMessage.WDict d, MTree tree, Date bday, Date eday) {
		String dictPath = MetaConstants.ZK_PATH_META_DICT + "/" + d.getDictName();
		MTableNode dict = loadTableNode(dictPath, bday, eday);
		if(dict != null) tree.getDictTree().put(dict.getName(), dict);
		for(WFieldMessage.WDict child: d.getChildDictList()) loadDict(child, tree, bday, eday);
	}

	/**
	 * load nested rollups
	 */
	private MTree loadCube(WViewMessage.WCube cube, Date bday, Date eday) {
		MTree tree = new MTree();
		String rPath = MetaConstants.ZK_PATH_META_TABLE + "/" + cube.getName();
		MTableNode rTable = loadTableNode(rPath, bday, eday);
		if(rTable != null)
			tree.getTableTree().put(rTable.getName(), rTable);
		for(WViewMessage.WCube r: cube.getCubesList()){
			tree.union(loadCube(r, bday, eday));
		}
		return tree;
	}

	// load view node
	private MViewNode loadViewNode(String viewPath) {
		MViewNode viewNode = null;
		try {
			byte[] data = client.getData(viewPath);
			if (data != null) {
				WViewMessage.WView.Builder builder = WViewMessage.WView.newBuilder();
				JsonFormat.merge(new String(data), builder);
				WViewMessage.WView view = builder.build();
				viewNode = new MViewNode(view.getName(), view);
			} else
				log.warn("Empty view: " + viewPath);
		} catch (ParseException e){
			log.error("load view node error: (" + viewPath +") " + ExceptionUtils.getFullStackTrace(e));
		} catch (Exception e) {
			log.error("load view node error: (" + viewPath +") "  + ExceptionUtils.getFullStackTrace(e));
		}
		return viewNode;
	}

	// load table node
	private MTableNode loadTableNode(String tablePath, Date bday, Date eday) {
		MTableNode tableNode = null;
		try {
			byte[] data = client.getData(tablePath);
			if (data != null) {
				// table
				WTableMessage.WTable.Builder builder = WTableMessage.WTable.newBuilder();
				JsonFormat.merge(new String(data), builder);
				WTableMessage.WTable table = builder.build();
				String tableName = table.getTableName();
				tableNode = new MTableNode(tableName, table);
				// check cluster
				if(!MetaConstants.CLUSTER_LEVEL_ALL.equalsIgnoreCase(level) && !table.getLevelList().contains(level))
					return null;
				// check valid
				if(table.getIsValid() != MetaConstants.META_STATUS_VALID) return null;
				// partition
				List<String> partitions = client.getChildren(tablePath);
				int startIndex = partitions.size() - 10 < 0 ? 0 : partitions.size() - 10;
				LogUtil.info(log, 100001, "partitions(" + tablePath + "): " + partitions.subList(startIndex, partitions.size()));
				Map<String, MPartitionNode> patitionMaps = updatePartitions(tableName, tablePath, partitions, bday, eday);
				tableNode.getPartitions().putAll(patitionMaps);
			} else log.warn("Empty table: " + tablePath);
		} catch (ParseException e){
			log.error("load table node error:  (" + tablePath +") "  + ExceptionUtils.getFullStackTrace(e));
		} catch (Exception e) {
			log.error("load table node error:  (" + tablePath +") "  + ExceptionUtils.getFullStackTrace(e));
		}
		return tableNode;
	}

	private Map<String, MPartitionNode> updatePartitions(String tableName, String tablePath, List<String> partitions, Date bday, Date eday) throws Exception {
	    Map<String, MPartitionNode> partitionMaps = Maps.newConcurrentMap();
	    for (String partition : partitions) {
            String partitionPath = tablePath + "/" + partition;
            MPartitionNode partitionNode = loadPartitionNode(tableName, partitionPath);
            if(partitionNode != null) {
				// if partition between bday and eday, add partition
				int start = TimeUtil.dateToInt(TimeUtil.stringToDate(partitionNode.getBaseTableNode().getTable().getStartTime()));
				int end = TimeUtil.dateToInt(TimeUtil.stringToDate(partitionNode.getBaseTableNode().getTable().getEndTime()));
				if(start <= TimeUtil.dateToInt(eday) && end >= TimeUtil.dateToInt(bday))
					partitionMaps.put(partition, partitionNode);
			}
        }
	    return partitionMaps;
    }

    // load partition node
	private MPartitionNode loadPartitionNode(String tableName, String partitionPath) throws Exception {
		MPartitionNode partitionNode = null;
		byte[] data = client.getData(partitionPath);
		if (data != null) {
			WTableMessage.WPartitionTable.Builder builder = WTableMessage.WPartitionTable.newBuilder();
			JsonFormat.merge(new String(data), builder);
			WTableMessage.WPartitionTable table = builder.build();
			String partitionName = tableName + MetaConstants.META_DELIM_NAME + table.getPartitionKey();
			// check is valid
			if(table.getIsValid() != MetaConstants.META_STATUS_VALID) return null;
			// base table
			WTableMessage.WBaseTable baseTable = null;
			String bPath = partitionPath + "/base";
			byte[] bData = client.getData(bPath);
			if (bData != null) {
				WTableMessage.WBaseTable.Builder bBuilder = WTableMessage.WBaseTable.newBuilder();
				JsonFormat.merge(new String(bData), bBuilder);
				baseTable = bBuilder.build();
			} else log.warn("Empty base table: " + bPath);
			String baseName = partitionName + MetaConstants.META_DELIM_NAME + "base";
			// tablet
			WTableMessage.WTablet tablet = null;
			if(baseTable != null) {
				String tPath = bPath + "/" + baseTable.getCurrentTablet();
				byte[] tData = client.getData(tPath);
				if (tData != null) {
					WTableMessage.WTablet.Builder tBuilder = WTableMessage.WTablet.newBuilder();
					JsonFormat.merge(new String(tData), tBuilder);
					tablet = tBuilder.build();
				} else log.warn("Empty base table: " + bPath);
			}
			String tabletName = baseName +  MetaConstants.META_DELIM_NAME + baseTable.getCurrentTablet();
			// node
			MTabletNode tabletNode = new MTabletNode(tabletName, tablet);
			MBaseTableNode baseNode = new MBaseTableNode(baseName, baseTable, tabletNode);
			partitionNode = new MPartitionNode(partitionName, table, baseNode);
		}
		return partitionNode;
	}
}


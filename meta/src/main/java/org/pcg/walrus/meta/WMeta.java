package org.pcg.walrus.meta;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.meta.register.MetaLoader;
import org.pcg.walrus.meta.register.MetaRegister;
import org.pcg.walrus.meta.tree.MTableNode;
import org.pcg.walrus.meta.tree.MTree;
import org.pcg.walrus.meta.tree.MViewNode;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.common.io.ZkClient;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WMeta
 */
public class WMeta {

	private static final Logger log = LoggerFactory.getLogger(WMeta.class);
	// env
	private ZkClient client;
	// register
	private MetaContainer container;
	private boolean ifReg = Boolean.FALSE;
	private MetaLoader loader;
	private MetaRegister register;

	public WMeta(WContext context, ZkClient client) {
		this(context, client, MetaConstants.CLUSTER_LEVEL_ALL, null, Boolean.TRUE);
	}

	/**
	 * for meta service
	 */
	public WMeta(String level, ZkClient client) {
		this.client = client;
		container = new MetaContainer();
		loader = new MetaLoader(client, level);
	}

	/**
	 * for calculation
	 */
	public WMeta(WContext context, ZkClient client, String cluster, String cacheLevel, boolean ifReg) {
		this(cluster, client);
		this.ifReg = ifReg;
		if(ifReg) register = new MetaRegister(context, cacheLevel);
	}

	// load meta asynchronously
	public void load() throws WMetaException {
		load(Boolean.TRUE, Boolean.TRUE);
	}

	/**
	 * @param async if register tablet asynchronously
	 * @throws WMetaException
	 */
	public void load(boolean async, boolean ifMonitor) throws WMetaException {
		try {
			loader.ensureMetaPath();
			MTree tree = loader.loadAll();
			if(async && ifReg) register.startRegisterService(TimeUtil.defaultBday(), TimeUtil.defaultEday());
			updateMeta(tree, async, TimeUtil.defaultBday(), TimeUtil.defaultEday());
		} catch (Exception e) {
			log.error("load meta error: " + ExceptionUtils.getFullStackTrace(e));
			throw new WMetaException(e.getMessage());
		}
		if(ifMonitor) monitorMeta();
	}

	// load view
	public void loadView(String view, Date bday, Date eday, boolean async) throws WMetaException {
		try {
			loader.ensureMetaPath();
			MTree tree = loader.loadView(view, bday, eday);
			updateMeta(tree, async, bday, eday);
		} catch (Exception e) {
			log.error("load view error: " + ExceptionUtils.getFullStackTrace(e));
			throw new WMetaException(e.getMessage());
		}
	}

	// load table
	@Deprecated
	public void loadTable(String table, Date bday, Date eday) throws WMetaException {
		try {
			loader.ensureMetaPath();
			MTree tree = loader.loadTable(table, bday, eday);
			updateMeta(tree, Boolean.FALSE, bday, eday);
		} catch (Exception e) {
			log.error("load table error: " + ExceptionUtils.getFullStackTrace(e));
			throw new WMetaException(e.getMessage());
		}
	}

	// view tree
	@Deprecated
	public MTree getMetaTree() throws WMetaException {
		return container.currentTree();
	}

	// view tree
	public Map<String, MViewNode> getViews() throws WMetaException {
		checkLoaded();
		return container.currentTree().getViewTree();
	}

	// table tree
	public Map<String, MTableNode> getTables() throws WMetaException {
		checkLoaded();
		return container.currentTree().getTableTree();
	}

	// dict tree
	public Map<String, MTableNode> getDicts() throws WMetaException {
		checkLoaded();
		return container.currentTree().getDictTree();
	}

	// if table is loaded
	private void checkLoaded()  throws WMetaException {
		if(!container.isLoaded()) throw new WMetaException("WMeta is not initialized!");
	}

	// rollback to last version
	public void rollback() {
		container.switchTree();
	}

	/**
	 * if tablet not ready, register tablet
	 */
	public boolean mkSureTableLoaded(Set<String> tablets) {
		for(String tablet: tablets) {
			try {
				if(!register.isTableReady(tablet)) register.registerTable(tablet);
			} catch (Exception e){
				log.error("register table error: " + ExceptionUtils.getFullStackTrace(e));
				return false;
			}
		}
		return true;
	}

	/*
	 * monitor meta
	 */
	private void monitorMeta() {
		try {
			// watch /walrus/meta/update node and apply each new update
			if(!client.exists(MetaConstants.ZK_PATH_UPDATE))
				client.setData(MetaConstants.ZK_PATH_UPDATE, "walrus_meta_update".getBytes());
			client.watchChildren(MetaConstants.ZK_PATH_UPDATE, new ZkClient.ZkListener() {
				@Override
				public void onNodeEvent(ChildData data) {}
				@Override
				public void onChildEvent(Type type, ChildData data) {
					try {
						// if add node, apply update
						if(type.equals(Type.CHILD_ADDED)) onMetaUpdate(data.getPath());
					} catch (WMetaException e) {
						log.error("update meta error: " + ExceptionUtils.getFullStackTrace(e));
					}
				}
			});
		} catch (Exception e) {
			log.error("monitor zk error: " + ExceptionUtils.getFullStackTrace(e));
		}
	}

	/**
	 * 1 copy a current tree
	 * 2 apply update to this copy tree
	 * @throws WMetaException
	 */
	private synchronized void onMetaUpdate(String updatePath) throws WMetaException {
		try {
			MTree tree = container.copyTree();
			// parse json
			String data = client.getString(updatePath);
			int tryTime = 0;
			while(StringUtils.isBlank(data) && tryTime++ < 60) {
				Thread.sleep(10);
				data = client.getString(updatePath);
			}
			log.debug("[SHARK Meta]update meta data: " + data);
			JSONObject json = (JSONObject) new JSONParser().parse(data);
			long uTime = (Long) json.getOrDefault("uTime",System.currentTimeMillis() - (1000 * 3600));
			// if update time is one hour ago, just ignore
			if((System.currentTimeMillis() - uTime) / 1000 >= 3600) {
				client.delete(updatePath);
				return;
			}
			log.info("[SHARK Meta]update meta: " + updatePath);
			String oper = (String) json.getOrDefault("oper","");
			String type = (String) json.getOrDefault("type","");
			String name = (String) json.getOrDefault("name","");
			String partition = (String) json.getOrDefault("partition","");
			// if flush, reload and return
			if("flush".equalsIgnoreCase(oper)) {
				if(register != null) register.flush();
				load(Boolean.TRUE, Boolean.FALSE);
			} else {
				loader.updateTree(oper, tree, type, name, partition);
				updateMeta(tree, Boolean.TRUE, TimeUtil.defaultBday(), TimeUtil.defaultEday());
			}
		} catch (Exception e) {
			log.error("load meta error: " + ExceptionUtils.getFullStackTrace(e));
			throw new WMetaException(e.getMessage());
		}
	}

	// unregist useless tablets, regist all tablets, update tree,
	private void updateMeta(MTree tree, boolean async, Date bday, Date eday) throws WMetaException {
		if(ifReg) {
			register.unregister();
			register.register(tree.getRContainers(), async, bday, eday);
		}
		container.update(tree);
		log.info("[SHARK Meta]FINISH LOAD META: " + tree);
	}


}

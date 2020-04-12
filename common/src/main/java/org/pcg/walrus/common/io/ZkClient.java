package org.pcg.walrus.common.io;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClient {

	private static final Logger log = LoggerFactory.getLogger(ZkClient.class);

	private CuratorFramework client;

	// for lock
	private CountDownLatch countDownLatch = new CountDownLatch(1);


	private String url;
	private int timeout;
	
	public ZkClient(String url, int timeout) {
		this.url = url;
		this.timeout = timeout;
	}

	/**
	 * start client
	 */
	public boolean connect() {
		try {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(timeout, 3);
			client = CuratorFrameworkFactory.newClient(url, retryPolicy);
			client.start();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * close client
	 */
	public void close() {
		if(client != null) client.close();
	}

	// setData
	public void setData(String path, byte[] data) throws Exception {
		if(!exists(path)) client.create().creatingParentsIfNeeded().forPath(path, data);
		else client.setData().forPath(path, data);
	}

	// get string
	public String getString(String path) throws Exception {
		return new String(getData(path));
	}

	// get byte
	public byte[] getData(String path) throws Exception {
		return client.getData().forPath(path);
	}

	public boolean delete(String path) {
		try {
			client.delete().deletingChildrenIfNeeded().forPath(path);
		} catch (Exception e) {
			log.error("zk delete path error: " + e.getMessage());
			return false;
		}
		return true;
	}

	// exist
	public boolean exists(String path) {
		try {
			Stat stat = client.checkExists().forPath(path);
			return stat != null;
		} catch (Exception e) {
			log.error("zk delete path error: " + e.getMessage());
			return false;
		}
	}

	// get children
	public List<String> getChildren(String path) throws Exception {
		return client.getChildren().forPath(path);
	}

	/**
	 * watch child
	 */
	public void watchChildren(String path, ZkListener listener) throws Exception {
		@SuppressWarnings("resource")
		PathChildrenCache cache = new PathChildrenCache(client, path, false);
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
					throws Exception {
				Type type = event.getType();
				ChildData data = event.getData();
				listener.onChildEvent(type, data);
			}
		});
		cache.start();
	}

	/**
	 * try lock once
	 */
	public boolean tryLock(String path, String data) {
		boolean success = false;
		try {
			client.create()
					.creatingParentsIfNeeded()
					.withMode(CreateMode.EPHEMERAL)
					.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
					.forPath(path, data.getBytes());
			log.info("success to acquire lock for path:{}", path);
			success = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return success;
	}

	/**
	 * lock until success
	 */
	public void lock(String path) {
		while (true) {
			if(tryLock(path, "")) break;
			else {
				log.info("while try again .......");
				try {
					if (countDownLatch.getCount() <= 0) {
						countDownLatch = new CountDownLatch(1);
					}
					countDownLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * unlock
	 */
	public boolean unlock(String path) {
		try {
			if (client.checkExists().forPath(path) != null) {
				client.delete().forPath(path);
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	/**
	 * zk watch listener
	 */
	public interface ZkListener {

		/**
		 * called when node updated/deleted
		 */
		public void onNodeEvent(ChildData data);


		/**
		 * called when child add/updated/deleted
		 */
		public void onChildEvent(Type type, ChildData data);
	}
}

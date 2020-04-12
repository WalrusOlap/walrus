package org.pcg.walrus.meta.register;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import org.apache.spark.storage.StorageLevel;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.common.util.WalrusUtil;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.common.util.TimeUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.pcg.walrus.meta.register.source.ISource;
import org.pcg.walrus.meta.register.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaRegister regist meta data to calculate context</br>
 * e.g. spark sql context(regist template)...
 */
public class MetaRegister {

	private static final Logger log = LoggerFactory.getLogger(MetaRegister.class);
	private static int REG_THREADS = 10;

	private WContext context;
	private Map<String, RegUnit> containerMap;
	private LinkedBlockingQueue<String> _q_containers;
	
	private String cacheLevel;

	public static int R_STATUS_INIT = 0; // initial status
	public static int R_STATUS_STANDBY = 1; // wait for registing
	public static int R_STATUS_READY = 2; // registered
	public static int R_STATUS_TO_RETIRE = 3; // to unregist

	public MetaRegister(WContext context, String cacheLevel) {
		this.context = context;
		this.cacheLevel = cacheLevel;
		containerMap = new ConcurrentHashMap<>();
		_q_containers = new LinkedBlockingQueue<>(10000);
	}

	/**
	 * @throws WMetaException
	 */
	public synchronized void register(List<RegUnit> containers, boolean async, Date bday, Date eday)
			throws WMetaException {
		log.info("[SHARK_META]start register job: " + containers.size());
		// if original tablet in containerMap not exist in containers: update status to R_STATUS_TO_RETIRE
		for(RegUnit container: containerMap.values()) {
			if (!containers.contains(container)) {
				container.setStatus(R_STATUS_TO_RETIRE);
			}
		}
		// if tablet not register, update status to R_STATUS_STANDBY
		for(RegUnit container: containers){
			if (!containerMap.containsKey(container.getName())) {
				container.setStatus(R_STATUS_STANDBY);
				containerMap.put(container.getName(), container);
			} else {
				RegUnit c = containerMap.get(container.getName());
				if ((c.getStatus() != R_STATUS_READY && c.getStatus() != R_STATUS_STANDBY)
						|| !c.getSignature().equals(container.getSignature())) {
					log.info("[Signature compare]" + container.getName() + ": " + c.getSignature() + ", " + container.getSignature());
					container.setStatus(R_STATUS_STANDBY);
					containerMap.put(container.getName(), container);
				}
			}
		}
		// register
		if(async) {
			try {
				for(RegUnit c: containerMap.values())
					if(c.getStatus() == R_STATUS_STANDBY) _q_containers.put(c.getName());
			} catch (Exception e) {
				log.error("register table error: " + e.getMessage());
			}
		}
		else parallellyRegister(bday, eday);
	}

	/**
	 * unregister all contanier which is marked R_STATUS_TO_RETIRE
	 */
	@SuppressWarnings("unchecked")
	public synchronized void unregister() {
		new Thread() {
			public void run() {
				// TODO unregister tablet safely
				try {
					Thread.sleep(1000 * 60 * 10);
				} catch (InterruptedException e) {
					log.error("unregister error: sleep error!");
				}
				// calculate priority
				for(RegUnit container: containerMap.values()) {
					if(R_STATUS_TO_RETIRE == container.getStatus()) {
						log.info("unregister tablet: " + container.getName());
						Dataset<Row> df = container.getRegistData();
						if(df != null) df.unpersist(false);
						containerMap.remove(container.getName());
					}
				}
			};
		}.start();
	}

	// register tablet
	public void registerTable(String tablet) throws WMetaException {
		try {
			Date bday = TimeUtil.intToDate(MetaConstants.DEFAULT_BDAY);
			Date eday = TimeUtil.intToDate(MetaConstants.DEFAULT_EDAY);
			register(tablet, bday, eday);
		} catch (Exception e) {
			throw new WMetaException("registerTable error: " + e.getMessage());
		}
	}

	// if tablet has been registered
	public boolean isTableReady(String tablet) {
		return containerMap.containsKey(tablet)
				&& containerMap.get(tablet).getStatus() == R_STATUS_READY;
	}

	/**
	 * start a new thread to register tables
	 */
	public void startRegisterService(final Date bday, final Date eday) {
		new Thread() {
			public void run() {
				while (true) {
					try {
						String container = _q_containers.take();
						if(container != null) register(container, bday, eday);
					} catch (InterruptedException e) {
						log.error("register error: " + ExceptionUtils.getFullStackTrace(e));
					} catch (Exception e) {
						log.error("register error: " + ExceptionUtils.getFullStackTrace(e));
					}
				}
			};
		}.start();
	}

	/**
	 * start ${REG_THREADS} to register tables
	 */
	private void parallellyRegister(Date bday, Date eday) {
		// register in REG_THREADS threads
		ExecutorService executor = Executors.newFixedThreadPool(REG_THREADS);
		for (String tablet : containerMap.keySet()) {
			executor.execute(new Thread() {
				@Override
				public void run() {
					try {
						register(tablet, bday, eday);
					} catch (WMetaException e) {
						log.error("register error: " + ExceptionUtils.getFullStackTrace(e));
					} catch (Exception e) {
						log.error("register error: " + ExceptionUtils.getFullStackTrace(e));
					}
				}
			});
		}
		executor.shutdown();
		try {
			// wait until register finished
			while (!executor.awaitTermination(1, TimeUnit.SECONDS));
		} catch (InterruptedException e) {
			log.error("register error: " + ExceptionUtils.getFullStackTrace(e));
		}
	}

	/**
	 * load schema from hdfs/druid/kudu and register
	 * @param tablet
	 * @throws WMetaException
	 */
	private void register(String tablet, Date bday, Date eday) throws WMetaException {
		if(!containerMap.containsKey(tablet)) return;
		RegUnit container = containerMap.get(tablet);
		if(container.getStatus() != R_STATUS_STANDBY ) return;
		String format = container.getTablet().getFormat();
		String path = container.getTablet().getPath();
		WTableMessage.WTablet t = container.getTablet();
		log.info("register tablet(" + format + "): " + tablet + ": " + path);
		ISource source = SourceFactory.loadSource(format);
		Dataset<Row> df = source.register(context, t);
		if(df != null) {
			// broadcast
			if(container.getTablet().getIsBc() == 1) {
				log.info("broadcast table: " + tablet);
				String logic = container.getTablet().getBcLogic();
				String table = tablet + "_tmp";
				df.createOrReplaceTempView(table);
				// BC LOGIC
				if(StringUtils.isNotEmpty(logic)) {
					logic = logic.replace("{BDAY}", TimeUtil.DateToString(bday));
					logic = logic.replace("{EDAY}", TimeUtil.DateToString(eday));
					logic = logic.replace("{TABLE}", table);
				} else {
					logic = "SELECT * FROM " + table;
				}
				// bc
				log.info(String.format("broadcast table %s: %s", tablet, logic));
				WalrusUtil.cacheTable(context.getSqlContext(), context.getSqlContext().sql(logic), tablet);
			} else df.createOrReplaceTempView(tablet);
			// cache df
			if(storageLevel() != null) df.persist(storageLevel());
			container.setRegistData(df);
		}		
		container.setStatus(R_STATUS_READY);
	}

	// cache level
	private StorageLevel storageLevel(){
		switch (cacheLevel) {
			case "memory":
				return StorageLevel.MEMORY_AND_DISK_SER();
			default:
				return null;
		}
	}

	/**
	 * clean containerMap
	 */
	public void flush() {
		containerMap.clear();
	}

}

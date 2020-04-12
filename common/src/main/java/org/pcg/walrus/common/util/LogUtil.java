package org.pcg.walrus.common.util;

import org.slf4j.Logger;

public class LogUtil {

	// info
	public static void debug(Logger log, long jobId, String msg) {
		log.debug("[" + jobId + "]" + msg);
	}

	// info
	public static void info(Logger log, long jobId, String msg) {
		log.info("[" + jobId + "]" + msg);
	}
	
	// warn
	public static void warn(Logger log, long jobId, String msg) {
		log.warn("[" + jobId + "]" + msg);
	}
	
	// error
	public static void error(Logger log, long jobId, String msg) {
		log.error("[" + jobId + "]" + msg);
	}
}

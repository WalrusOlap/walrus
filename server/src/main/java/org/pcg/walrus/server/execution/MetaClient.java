package org.pcg.walrus.server.execution;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.pcg.walrus.common.exception.WMetaException;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.common.io.ZkClient;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.WMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class MetaClient implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MetaClient.class);

    private @Value("${zk.url}") String zkUrl;
    private @Value("${zk.timeout}") int zkTimeout;

    private WMeta meta;
    private ZkClient client;

    private volatile boolean loaded = false;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String runScope = "running";
        String env = System.getProperty("walrus.scope", runScope);
        if(runScope.equalsIgnoreCase(env)) connect();
    }

    /**
     *
     * @throws WServerException
     * @throws WMetaException
     */
    public void connect() {
        try {
            client = new ZkClient(zkUrl, zkTimeout);
            if(!client.connect()) throw new WServerException("connect to zk failed!");
            log.info(String.format("connecting to zk %s!", zkUrl));
            meta = new WMeta(MetaConstants.CLUSTER_LEVEL_ALL, client);
            meta.load();
            loaded = true;
        } catch (Exception e) {
            log.error("load meta error: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

    /**
     * @return zk
     */
    public ZkClient getZkClient() throws WServerException {
        if(!loaded) throw new WServerException("meta is uninitialized， please try later!");
        return client;
    }


    /**
     * @return WMeta
     */
    public WMeta getClient() throws WServerException {
        if(!loaded) throw new WServerException("meta is uninitialized， please try later!");
        return meta;
    }
}

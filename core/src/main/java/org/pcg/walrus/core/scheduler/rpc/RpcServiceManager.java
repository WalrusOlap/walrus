package org.pcg.walrus.core.scheduler.rpc;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.common.io.ZkClient;
import org.pcg.walrus.meta.MetaConstants;

import java.util.ArrayList;
import java.util.List;
import java.net.InetAddress;

/**
 * rpc service management
 */
public class RpcServiceManager {

    // register Server on zk
    public static void registerRunner(ZkClient client, String level, InetAddress address, int port) throws WServerException
    {
        String zkPath = String.format("%s/%s_%s_%d", MetaConstants.ZK_PATH_RPC, level, address.getHostAddress(), port);
        if(!client.tryLock(zkPath, "")) throw new WServerException("register server failed: " + zkPath);
    }

    // register job zk
    public static void registerJob(ZkClient client, String level, InetAddress address, int port, long job) throws WServerException
    {
        String zkPath = String.format("%s/%d", MetaConstants.ZK_PATH_JOB, job);
        JSONObject data = new JSONObject();
        data.put("level", level);
        data.put("host", address.getHostAddress());
        data.put("port", port);
        if(!client.tryLock(zkPath, data.toJSONString()))
            throw new WServerException(String.format("more than one instance of job %d is running", job));
    }

    // register job zk
    public static void unRegisterJob(ZkClient client, long job) throws WServerException
    {
        String zkPath = String.format("%s/%d", MetaConstants.ZK_PATH_JOB, job);
        client.delete(zkPath);
    }

    // get task runner
    public static SharkRunner getTaskRunner(ZkClient client, long job) throws Exception
    {
        String zkPath = String.format("%s/%d", MetaConstants.ZK_PATH_JOB, job);
        JSONObject data = (JSONObject) new JSONParser().parse(client.getString(zkPath));
        long port = (long) data.get("port");
        return new SharkRunner(data.get("level").toString(), InetAddress.getByName(data.get("host").toString()), (int) port);
    }

    /**
     * get live server on zk
     */
    public static List<SharkRunner> availableRunners(ZkClient client, String level) throws Exception {
        List<String> children = client.getChildren(MetaConstants.ZK_PATH_RPC);
        List<SharkRunner> servers = new ArrayList<>();
        for(String server: children) {
            String[] hostPort = server.split("_");
            try {
                if(hostPort[0].equalsIgnoreCase(level)) {
                    SharkRunner runner = new SharkRunner(level, InetAddress.getByName(hostPort[1]), Integer.parseInt(hostPort[2]));
                    servers.add(runner);
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
        return servers;
    }

    /**
     * runner register on zk
     */
    public static class SharkRunner {
        public String level;
        public InetAddress address;
        public int port;
        public SharkRunner(String level, InetAddress address, int port){
            this.level = level;
            this.address = address;
            this.port = port;
        }
    }
}

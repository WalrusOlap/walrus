package org.pcg.walrus.core.scheduler.rpc;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * rpc message
 */
public class RpcMessage {

    // methods
    public static final String METHOD_RUN = "run";
    public static final String METHOD_KILL = "kill";
    // key
    private static final String KEY_METHOD = "method";
    private static final String KEY_TASK = "task";

    public String method;
    public long taskId;

    public RpcMessage(String method, long taskId) {
        this.method = method;
        this.taskId = taskId;
    }

    /**
     * @return json message
     */
    public String getMessage() {
        JSONObject msg = new JSONObject();
        msg.put(KEY_METHOD, method);
        msg.put(KEY_TASK, taskId);
        return msg.toJSONString();
    }

    /**
     * parse string to {RpcMessage}
     */
    public static RpcMessage parse(String json) throws ParseException  {
        JSONObject msg = (JSONObject) new JSONParser().parse(json);
        String method = (String) msg.get(KEY_METHOD);
        Long taskId = (Long) msg.get(KEY_TASK);
        return new RpcMessage(method, taskId);
    }
}

package org.pcg.walrus.examples;

import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TaskClient extends WalrusClient {

    public JSONObject query(String task) throws Exception {
        JSONObject json = readJson(String.format("/query/%s", task));
        String url = "task/query";
        Map<String, String> params = new HashMap<>();
        params.put("author", json.getOrDefault("author", "").toString());
        params.put("saveMode", json.getOrDefault("saveMode", "").toString());
        params.put("savePath", json.getOrDefault("savePath", "").toString());
        params.put("parameter", json.getOrDefault("parameter", "").toString());
        JSONAware logic = (JSONAware) json.get("logic");
        params.put("logic", logic.toJSONString());
        return post(url, params);
    }

    public JSONObject status(long task) throws Exception {
        String url = String.format("task/%d", task);
        return get(url);
    }

    // example entrance
    public static void main(String[] args) throws Exception {
        int threads = 5;
        ExecutorService workers = Executors.newFixedThreadPool(threads);
        TaskClient client = new TaskClient();
        File queries = new File(TaskClient.class.getResource("/query").toURI());
        for(File queryInfo: queries.listFiles()) {
            workers.execute(new Thread() {
                @Override
                public void run() {
                    try {
                        JSONObject ret = client.query(queryInfo.getName());
                        JSONObject task = (JSONObject) ret.get("data");
                        long taskId = (long) task.get("task_id");
                        while (true) {
                            JSONObject taskRet = client.status(taskId);
                            JSONObject json = (JSONObject) taskRet.get("data");
                            long status = (long) json.get("status");
                            if(status == 3 || status == 4) {
                                System.out.println(String.format("task %d finished: %s", taskId, task.toJSONString()));
                                break;
                            } else {
                                Thread.sleep(10 * 1000);
                                System.out.println(String.format("task %d running...", taskId));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        workers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
}

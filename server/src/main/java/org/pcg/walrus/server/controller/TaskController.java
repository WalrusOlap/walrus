package org.pcg.walrus.server.controller;

import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.server.model.ResponseTemplate;
import org.pcg.walrus.server.model.Task;
import org.pcg.walrus.server.service.TaskService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.apache.commons.httpclient.HttpStatus;

import java.util.List;

@RestController
@RequestMapping(value="/task")
public class TaskController {

    @Autowired
    private TaskService taskService;

    @PostMapping(value="/query")
    public ResponseTemplate<Task> query(@RequestParam(value = "author", required = true) String author,
            @RequestParam(value = "logic", required = true) String logic,
            @RequestParam(value = "saveMode", required = true) String saveMode,
            @RequestParam(value = "savePath", required = true) String savePath,
            @RequestParam(value = "saveFormat", required = false, defaultValue = "csv") String saveFormat,
            @RequestParam(value = "desc", required = false) String desc,
            @RequestParam(value = "submitType", required = false, defaultValue = "local") String submitType,
            @RequestParam(value = "type", required = false, defaultValue = "spark") String type,
            @RequestParam(value = "format", required = false, defaultValue = "json") String format,
            @RequestParam(value = "resource", required = false) String resource,
            @RequestParam(value = "parameter", required = false) String parameter,
            @RequestParam(value = "timeout", required = false, defaultValue = "7200") int timeout,
            @RequestParam(value = "ext", required = false) String ext) throws Exception {
        Task task = new Task();
        task.setAuthor(author);
        task.setLogic(logic);
        task.setSaveMode(saveMode);
        task.setSavePath(savePath);
        task.setSaveFormat(saveFormat);
        task.setDesc(desc);
        task.setSubmitType(submitType);
        task.setTaskType(type);
        task.setFormat(format);
        task.setResource(resource);
        task.setParameter(parameter);
        task.setTimeout(timeout);
        task.setExt(ext);
        taskService.query(task);
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(task);
        return response;
    }

    @GetMapping(value="/{taskId}")
    public ResponseTemplate<Task> getTask(@PathVariable int taskId) throws WServerException  {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        Task task = taskService.getTask(taskId);
        response.setData(task);
        return response;
    }

    @GetMapping(value="/author/{author}")
    public ResponseTemplate<List<Task>> getTask(@PathVariable String author,
                                                @RequestParam(value = "limit", required = false, defaultValue = "1000") int limit) {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(taskService.getTaskByAuthor(author, limit));
        return response;
    }

    @GetMapping(value="/recentTasks")
    public ResponseTemplate<List<Task>> recentTasks(@RequestParam(value = "limit", required = false, defaultValue = "1000") int limit) {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        response.setData(taskService.getRecentTasks(limit));
        return response;
    }

    @GetMapping(value="/kill/{taskId}")
    public ResponseTemplate<List<Task>> killTask(@PathVariable long taskId) throws Exception {
        ResponseTemplate response = new ResponseTemplate(HttpStatus.SC_OK);
        taskService.kill(taskId);
        return response;
    }


}

package org.pcg.walrus.server.service;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.pcg.walrus.common.exception.WException;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.server.execution.TaskClient;
import org.pcg.walrus.server.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.pcg.walrus.server.dao.TaskDao;

import java.util.List;

@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    @Autowired
    private TaskDao taskDao;

    @Autowired
    private TaskClient taskClient;

    /**
     * @param task
     */
    public void query(Task task) throws WServerException {
        // save db
        task.setStatus(CoreConstants.JOB_STATUS_WAITING);
        taskDao.insert(task);
        // submit task
        try {
            taskClient.submit(task);
        } catch (Exception e) {
            String msg = ExceptionUtils.getFullStackTrace(e);
            log.error(String.format("submit task error: %s", msg));
            taskDao.fail(task.getTaskId(), CoreConstants.JOB_STATUS_FAILED, WException.SYSTEM_ERROR, msg);
            throw new WServerException(msg);
        }
    }

    /**
     * @param task
     */
    public void kill(long task) throws Exception {
        taskClient.kill(task);
    }

    /**
     * recent tasks
     * @param limit
     */
    public List<Task> getRecentTasks(int limit) {
        return taskDao.getRecentTasks(limit);
    }

    /**
     * get tasks by author
     * @param author
     */
    public List<Task> getTaskByAuthor(String author, int limit) {
        return taskDao.getTaskByAuthor(author, limit);
    }

    /**
     * get task by id
     * @param id
     */
    public Task getTask(int id) throws WServerException {
        Task task =  taskDao.getTask(id);
        if(task == null) throw new WServerException("NO TASK FOUND: " + id);
        return task;
    }
}

package org.pcg.walrus.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {t_walrus_task}
 */
public class Task {

    @JsonProperty("task_id")
    private long taskId;
    private String author;
    private String level;
    private String format;
    private String desc;
    @JsonProperty("task_type")
    private String type;
    private String start;
    private String end;
    private String logic;
    private String table;
    private String bday;
    private String eday;
    private int status;
    private int timeout;
    @JsonProperty("save_mode")
    private String saveMode;
    @JsonProperty("save_format")
    private String saveFormat;
    @JsonProperty("save_path")
    private String savePath;
    @JsonProperty("result_lines")
    private long resultLines;
    @JsonProperty("error_code")
    private int errorCode;
    private String message;
    private String ext;
    @JsonProperty("submit_type")
    private String submitType;
    @JsonProperty("task_plan")
    private String taskPlan;
    private String resource;
    private String parameter;

    // getter and setter

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getType() {
        return type;
    }

    public void setTaskType(String type) {
        this.type = type;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getSaveMode() {
        return saveMode;
    }

    public void setSaveMode(String saveMode) {
        this.saveMode = saveMode;
    }

    public String getSavePath() {
        return savePath;
    }

    public void setSavePath(String savePath) {
        this.savePath = savePath;
    }

    public long getResultLines() {
        return resultLines;
    }

    public void setResultLines(long resultLines) {
        this.resultLines = resultLines;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getExt() {
        return ext;
    }

    public void setExt(String ext) {
        this.ext = ext;
    }

    public String getSubmitType() {
        return submitType;
    }

    public void setSubmitType(String submitType) {
        this.submitType = submitType;
    }

    public String getTaskPlan() {
        return taskPlan;
    }

    public void setTaskPlan(String taskPlan) {
        this.taskPlan = taskPlan;
    }

    public String getSaveFormat() {
        return saveFormat;
    }

    public void setSaveFormat(String saveFormat) {
        this.saveFormat = saveFormat;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getBday() {
        return bday;
    }

    public void setBday(String bday) {
        this.bday = bday;
    }

    public String getEday() {
        return eday;
    }

    public void setEday(String eday) {
        this.eday = eday;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}

package org.pcg.walrus.server.model;

/**
 * api return template
 * @param <T>
 */
public class ResponseTemplate<T> {

    private int status; // similar to http status
    private String message = "successed";
    private T data;

    public ResponseTemplate(int status) {
        setStatus(status);
    }

    public ResponseTemplate(int status, String message) {
        setStatus(status);
        setMessage(message);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}

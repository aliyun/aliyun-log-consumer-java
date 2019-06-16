package com.aliyun.openservices.loghub.client;

public class TaskResult {

    private Exception exception;

    public Exception getException() {
        return exception;
    }

    public TaskResult(Exception exception) {
        this.exception = exception;
    }
}

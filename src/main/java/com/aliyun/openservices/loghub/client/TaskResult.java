package com.aliyun.openservices.loghub.client;

public class TaskResult {

    private Exception mException;

    public Exception getException() {
        return mException;
    }

    public TaskResult(Exception e) {
        mException = e;
    }
}

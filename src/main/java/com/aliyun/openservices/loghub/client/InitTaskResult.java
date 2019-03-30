package com.aliyun.openservices.loghub.client;

public class InitTaskResult extends TaskResult {

    private String cursor;
    private boolean cursorPersistent;

    public InitTaskResult(String cursor, boolean cursorPersistent) {
        super(null);
        this.cursor = cursor;
        this.cursorPersistent = cursorPersistent;
    }

    public String getCursor() {
        return cursor;
    }

    public boolean isCursorPersistent() {
        return cursorPersistent;
    }
}

package com.aliyun.openservices.loghub.client;

public class ProcessTaskResult extends TaskResult {

    private String rollBackCheckPoint;

    public ProcessTaskResult(String rollbackCheckPoint) {
        super(null);
        this.rollBackCheckPoint = rollbackCheckPoint;
    }

    public String getRollBackCheckpoint() {
        return rollBackCheckPoint;
    }
}

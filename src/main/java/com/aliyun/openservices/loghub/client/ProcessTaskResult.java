package com.aliyun.openservices.loghub.client;

public class ProcessTaskResult extends TaskResult {

	private String mRollBackCheckPoint = "";
    public String getRollBackCheckpoint()
    {
    	return mRollBackCheckPoint;
    }
	
	public ProcessTaskResult(String rollbackCheckPoint) {
		super(null);
		mRollBackCheckPoint = rollbackCheckPoint;
	}

}

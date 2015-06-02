package com.aliyun.openservices.loghub.client;

public class LogHubCheckPoint {
	private String mMemoryCheckPoint;
	private String mProcessingCursor;

	public void setProcessingCursor(String processingCursor)
	{
		mProcessingCursor = processingCursor;
		
	}
	public void updateLocalCheckPoint()
	{
		mMemoryCheckPoint = mProcessingCursor;
	}
	
	public String getLocalCheckPoint()
	{
		return mMemoryCheckPoint;
	}
}

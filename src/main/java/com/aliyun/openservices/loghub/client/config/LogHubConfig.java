package com.aliyun.openservices.loghub.client.config;

import java.io.Serializable;

public class LogHubConfig implements Serializable {
	private static final long serialVersionUID = -460559812263406428L;

	public static final long DEFAULT_DATA_FETCH_INTERVAL_MS = 200;
	private String mConsumerGroupName;
	private String mWorkerInstanceName;
	private String mLogHubEndPoint;
	private String mProject;
	private String mLogStore;
	private String mAccessId;
	private String mAccessKey;
	private LogHubCursorPosition mCursorPosition;
	private int  mLoghubCursorStartTime = 0;
	private long mDataFetchIntervalMillis;
	private long mHeartBeatIntervalMillis;
	private boolean mConsumeInOrder;
	public LogHubConfig(String consumerGroupName, String workerInstanceName, String loghubEndPoint, 
			String project, String logStore,
			String accessId, String accessKey,
			LogHubCursorPosition cursorPosition,
			long heartBeatIntervalMillis, 
			boolean consumeInOrder)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = workerInstanceName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mCursorPosition = cursorPosition;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
		mHeartBeatIntervalMillis = heartBeatIntervalMillis;
		mConsumeInOrder = consumeInOrder;
	}
	
	public LogHubConfig(String consumerGroupName, String workerInstanceName, String loghubEndPoint, 
			String project, String logStore,
			String accessId, String accessKey,
			int start_time,
			long heartBeatIntervalMillis,
			boolean consumeInOrder)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = workerInstanceName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mCursorPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
		mLoghubCursorStartTime = start_time;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
		mHeartBeatIntervalMillis = heartBeatIntervalMillis;
		mConsumeInOrder = consumeInOrder;
	}
	
	public long getDataFetchIntervalMillis() {
		return mDataFetchIntervalMillis;
	}

	public void setDataFetchIntervalMillis(long dataFetchIntervalMillis) {
		this.mDataFetchIntervalMillis = dataFetchIntervalMillis;
	}

	public boolean isConsumeInOrder() {
		return mConsumeInOrder;
	}

	public long getHeartBeatIntervalMillis() {
		return mHeartBeatIntervalMillis;
	}

	public String getConsumerGroupName()
	{
		return mConsumerGroupName;
	}
	public String getWorkerInstanceName()
	{
		return mWorkerInstanceName;
	}
	
	public String getLogHubEndPoint()
	{
		return mLogHubEndPoint;
	}
	public String getProject()
	{
		return mProject;
	}
	public String getLogStore()
	{
		return mLogStore;
	}
	public String getAccessId()
	{
		return mAccessId;
	}
	public String getAccessKey()
	{
		return mAccessKey;
	}
	
	public LogHubCursorPosition getCursorPosition()
	{
		return mCursorPosition;
	}
	
	public int GetCursorStartTime()
	{
		return mLoghubCursorStartTime;
	}
}

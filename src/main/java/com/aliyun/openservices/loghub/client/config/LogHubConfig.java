package com.aliyun.openservices.loghub.client.config;

import java.io.Serializable;

public class LogHubConfig implements Serializable {
	private static final long serialVersionUID = -460559812263406428L;

	public static final long DEFAULT_LEASE_DURATION_TIME_MS = 45 * 1000; // default 45 sec
	public static final long DEFAULT_DATA_FETCH_INTERVAL_MS = 500;
	private String mConsumerGroupName;
	private String mWorkerInstanceName;
	private String mLogHubEndPoint;
	private String mProject;
	private String mLogStore;
	private String mAccessId;
	private String mAccessKey;
	private LogHubCursorPosition mCursorPosition;
	private int  mLoghubCursorStartTime = 0;
	private long mLeaseDurationMillis;
	private long mDataFetchIntervalMillis;

	
	public LogHubConfig(String consumerGroupName, String workerInstanceName, String loghubEndPoint, 
			String project, String logStore,
			String accessId, String accessKey,
			LogHubCursorPosition cursorPosition)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = workerInstanceName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mCursorPosition = cursorPosition;
		mLeaseDurationMillis = DEFAULT_LEASE_DURATION_TIME_MS;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
	}
	
	public LogHubConfig(String consumerGroupName, String workerInstanceName, String loghubEndPoint, 
			String project, String logStore,
			String accessId, String accessKey,
			int start_time)
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
		mLeaseDurationMillis = DEFAULT_LEASE_DURATION_TIME_MS;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
	}
	
	public String getConsumerGroupName()
	{
		return mConsumerGroupName;
	}
	public String getWorkerInstanceName()
	{
		return mWorkerInstanceName;
	}
	
	public void setLeaseDurationTimeMillis(long durationMs)
	{
		this.mLeaseDurationMillis = durationMs;
	}
	public long getLeaseDurtionTimeMillis()
	{
		return mLeaseDurationMillis;
	}
	public void setDataFetchIntervalMillis(long fetchIntervalMs)
	{
		this.mDataFetchIntervalMillis = fetchIntervalMs;
	}
	public long getDataFetchIntervalMillis()
	{
		return this.mDataFetchIntervalMillis;
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

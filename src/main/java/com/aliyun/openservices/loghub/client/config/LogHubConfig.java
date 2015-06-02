package com.aliyun.openservices.loghub.client.config;

public class LogHubConfig {
	
	public static final long DEFAULT_LEASE_DURATION_TIME_MS = 15 * 1000; // default 15 sec
	public static final long DEFAULT_DATA_FETCH_INTERVAL_MS = 500;
	private String mConsumeGroupName;
	private String mWorkerInstanceName;
	private String mLogHubEndPoint;
	private int mLogHubPort;
	private String mLogHubProject;
	private String mLogHubStreamName;
	private String mAccessId;
	private String mAccessKey;
	private LogHubClientDbConfig mDbConfig;
	private LogHubCursorPosition mCursorPosition;
	private long mLeaseDurationMillis;
	private long mDataFetchIntervalMillis;

	
	public LogHubConfig(String consumeGroupName, String workerInstanceName, String loghubEndPoint, int loghubPort, 
			String loghubProject, String loghubStreamName,
			String accessId, String accessKey,
			LogHubClientDbConfig dbConfig, LogHubCursorPosition cursorPosition)
	{
		mConsumeGroupName = consumeGroupName;
		mWorkerInstanceName = workerInstanceName;
		mLogHubEndPoint = loghubEndPoint;
		mLogHubPort = loghubPort;
		mLogHubProject = loghubProject;
		mLogHubStreamName = loghubStreamName;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mDbConfig = dbConfig;
		mCursorPosition = cursorPosition;
		mLeaseDurationMillis = DEFAULT_LEASE_DURATION_TIME_MS;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
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
	public String getConsumeGroupName()
	{
		return mConsumeGroupName;
	}
	public String getWorkerInstanceName()
	{
		return mWorkerInstanceName;
	}
	public String getLogHubEndPoint()
	{
		return mLogHubEndPoint;
	}
	public int getLogHubPort()
	{
		return mLogHubPort;
	}
	public String getLogHubProject()
	{
		return mLogHubProject;
	}
	public String getLogHubStreamName()
	{
		return mLogHubStreamName;
	}
	public String getAccessId()
	{
		return mAccessId;
	}
	public String getAccessKey()
	{
		return mAccessKey;
	}
	public LogHubClientDbConfig getDbConfig()
	{
		return mDbConfig;
	}
	
	public LogHubCursorPosition getCursorPosition()
	{
		return mCursorPosition;
	}
}

package com.aliyun.openservices.loghub.client.config;

import java.io.Serializable;





public class LogHubConfig implements Serializable {
	public static enum ConsumePosition {BEGIN_CURSOR, END_CURSOR};
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
	private long mHeartBeatIntervalMillis = 20000;
	private boolean mConsumeInOrder = false;
	private String mStsToken = null;
	private boolean mUseDirectMode = false;
	private int mMaxFetchLogGroupSize = 1000;

	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
						String project, String logStore,
						String accessId, String accessKey,
						ConsumePosition position
						)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = consumerName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
		mHeartBeatIntervalMillis = 30000;
		mConsumeInOrder = false;
		mUseDirectMode = false;
		if(position == ConsumePosition.BEGIN_CURSOR) mCursorPosition = LogHubCursorPosition.BEGIN_CURSOR;
		else if(position == ConsumePosition.END_CURSOR) mCursorPosition = LogHubCursorPosition.END_CURSOR;
	}
	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
						String project, String logStore,
						String accessId, String accessKey,
						int consumerStartTimeInSeconds
	)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = consumerName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
		mHeartBeatIntervalMillis = 30000;
		mConsumeInOrder = false;
		mUseDirectMode = false;
		mCursorPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
		mLoghubCursorStartTime = consumerStartTimeInSeconds;
	}

	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
						String project, String logStore,
						String accessId, String accessKey,
						ConsumePosition position,
						int maxFetchLogGroupSize
	)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = consumerName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
		mHeartBeatIntervalMillis = 30000;
		mConsumeInOrder = false;
		mUseDirectMode = false;
		if(position == ConsumePosition.BEGIN_CURSOR) mCursorPosition = LogHubCursorPosition.BEGIN_CURSOR;
		else if(position == ConsumePosition.END_CURSOR) mCursorPosition = LogHubCursorPosition.END_CURSOR;
		mMaxFetchLogGroupSize = maxFetchLogGroupSize;
	}
	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
						String project, String logStore,
						String accessId, String accessKey,
						int consumerStartTimeInSeconds,
						int maxFetchLogGroupSize
	)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = consumerName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
		mHeartBeatIntervalMillis = 30000;
		mConsumeInOrder = false;
		mUseDirectMode = false;
		mCursorPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
		mLoghubCursorStartTime = consumerStartTimeInSeconds;
		mMaxFetchLogGroupSize = maxFetchLogGroupSize;
	}

	@Deprecated
	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
			String project, String logStore,
			String accessId, String accessKey,
			LogHubCursorPosition cursorPosition,
			long heartBeatIntervalMillis, 
			boolean consumeInOrder,
			boolean userDirectMode)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = consumerName;
		mLogHubEndPoint = loghubEndPoint;
		mProject = project;
		mLogStore = logStore;
		mAccessId = accessId;
		mAccessKey = accessKey;
		mCursorPosition = cursorPosition;
		mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
		mHeartBeatIntervalMillis = heartBeatIntervalMillis;
		mConsumeInOrder = consumeInOrder;
		this.mUseDirectMode = userDirectMode;
	}
	@Deprecated
	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
			String project, String logStore,
			String accessId, String accessKey,
			LogHubCursorPosition cursorPosition,
			long heartBeatIntervalMillis, 
			boolean consumeInOrder)
	{
		this(consumerGroupName, consumerName, loghubEndPoint, project, logStore, accessId, accessKey,
				cursorPosition, heartBeatIntervalMillis, consumeInOrder, false);
	}
	@Deprecated
	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
			String project, String logStore,
			String accessId, String accessKey,
			int start_time,
			long heartBeatIntervalMillis,
			boolean consumeInOrder)
	{
		mConsumerGroupName = consumerGroupName;
		mWorkerInstanceName = consumerName;
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
	@Deprecated
	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
			String project, String logStore,
			String accessId, String accessKey,
			LogHubCursorPosition cursorPosition,
			long heartBeatIntervalMillis, 
			boolean consumeInOrder, String stsToken)
	{
		this(consumerGroupName, consumerName, loghubEndPoint, project, logStore, accessId, accessKey, cursorPosition, heartBeatIntervalMillis, consumeInOrder);
		this.mStsToken = stsToken;
	}
	@Deprecated
	public LogHubConfig(String consumerGroupName, String consumerName, String loghubEndPoint,
			String project, String logStore,
			String accessId, String accessKey,
			int start_time,
			long heartBeatIntervalMillis,
			boolean consumeInOrder, String stsToken)
	{
		this(consumerGroupName, consumerName, loghubEndPoint, project, logStore, accessId, accessKey, start_time, heartBeatIntervalMillis, consumeInOrder);
		this.mStsToken = stsToken;
	}
	
	public String getStsToken() {
		return mStsToken;
	}

	public void setStsToken(String mStsToken) {
		this.mStsToken = mStsToken;
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
	public void setConsumeInOrder(boolean order) { mConsumeInOrder = order; }
	public long getHeartBeatIntervalMillis() {
		return mHeartBeatIntervalMillis;
	}
	public void setHeartBeatIntervalMillis(long heartBeatIntervalMillis) { this.mHeartBeatIntervalMillis = heartBeatIntervalMillis; }
	public String getConsumerGroupName()
	{
		return mConsumerGroupName;
	}
	public String getConsumerName() { return mWorkerInstanceName; }

	@Deprecated
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

	@Deprecated
	public void EnableDirectMode()
	{
		this.mUseDirectMode = true;
	}
	@Deprecated
	public void DisableDirectMode()
	{
		this.mUseDirectMode = false;
	}

	public void SetDirectMode(boolean enable) { this.mUseDirectMode = enable; }
	public boolean isDirectModeEnabled()
	{
		return this.mUseDirectMode;
	}

	public int getMaxFetchLogGroupSize() {
		return mMaxFetchLogGroupSize;
	}

	public void setMaxFetchLogGroupSize(int maxFetchLogGroupSize) {
		this.mMaxFetchLogGroupSize = maxFetchLogGroupSize;
	}
}

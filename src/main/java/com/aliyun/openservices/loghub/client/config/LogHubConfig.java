package com.aliyun.openservices.loghub.client.config;

import java.io.Serializable;


public class LogHubConfig implements Serializable {
    private static final long serialVersionUID = -460559812263406428L;

    public enum ConsumePosition {
        BEGIN_CURSOR,
        END_CURSOR
    }

    private static final long DEFAULT_DATA_FETCH_INTERVAL_MS = 200;

    private String mConsumerGroupName;
    private String consumerName;
    private String endpoint;
    private String mProject;
    private String mLogStore;
    private String mAccessId;
    private String mAccessKey;
    private LogHubCursorPosition mCursorPosition;
    private int mLoghubCursorStartTime = 0;
    private long mDataFetchIntervalMillis;
    private long mHeartBeatIntervalMillis;
    private boolean mConsumeInOrder;
    private String mStsToken = null;
    private boolean directModeEnabled = false;
    private int mMaxFetchLogGroupSize = 1000;
    private String userAgent;

    public LogHubConfig(String consumerGroupName,
                        String consumerName,
                        String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        ConsumePosition position) {
        mConsumerGroupName = consumerGroupName;
        this.consumerName = consumerName;
        this.endpoint = endpoint;
        mProject = project;
        mLogStore = logStore;
        mAccessId = accessId;
        mAccessKey = accessKey;
        mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
        mHeartBeatIntervalMillis = 30000;
        mConsumeInOrder = false;
        if (position == ConsumePosition.BEGIN_CURSOR) mCursorPosition = LogHubCursorPosition.BEGIN_CURSOR;
        else if (position == ConsumePosition.END_CURSOR) mCursorPosition = LogHubCursorPosition.END_CURSOR;
    }

    public LogHubConfig(String consumerGroupName, String consumerName, String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        int consumerStartTimeInSeconds) {
        mConsumerGroupName = consumerGroupName;
        this.consumerName = consumerName;
        this.endpoint = endpoint;
        mProject = project;
        mLogStore = logStore;
        mAccessId = accessId;
        mAccessKey = accessKey;
        mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
        mHeartBeatIntervalMillis = 30000;
        mConsumeInOrder = false;
        mCursorPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
        mLoghubCursorStartTime = consumerStartTimeInSeconds;
    }

    public LogHubConfig(String consumerGroupName, String consumerName, String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        ConsumePosition position,
                        int maxFetchLogGroupSize) {
        mConsumerGroupName = consumerGroupName;
        this.consumerName = consumerName;
        this.endpoint = endpoint;
        mProject = project;
        mLogStore = logStore;
        mAccessId = accessId;
        mAccessKey = accessKey;
        mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
        mHeartBeatIntervalMillis = 30000;
        mConsumeInOrder = false;
        if (position == ConsumePosition.BEGIN_CURSOR) mCursorPosition = LogHubCursorPosition.BEGIN_CURSOR;
        else if (position == ConsumePosition.END_CURSOR) mCursorPosition = LogHubCursorPosition.END_CURSOR;
        mMaxFetchLogGroupSize = maxFetchLogGroupSize;
    }

    public LogHubConfig(String consumerGroupName, String consumerName, String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        int consumerStartTimeInSeconds,
                        int maxFetchLogGroupSize) {
        mConsumerGroupName = consumerGroupName;
        this.consumerName = consumerName;
        this.endpoint = endpoint;
        mProject = project;
        mLogStore = logStore;
        mAccessId = accessId;
        mAccessKey = accessKey;
        mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
        mHeartBeatIntervalMillis = 30000;
        mConsumeInOrder = false;
        mCursorPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
        mLoghubCursorStartTime = consumerStartTimeInSeconds;
        mMaxFetchLogGroupSize = maxFetchLogGroupSize;
    }

    @Deprecated
    public LogHubConfig(String consumerGroupName, String consumerName, String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        LogHubCursorPosition cursorPosition,
                        long heartBeatIntervalMillis,
                        boolean consumeInOrder,
                        boolean directModeEnabled) {
        mConsumerGroupName = consumerGroupName;
        this.consumerName = consumerName;
        this.endpoint = endpoint;
        this.directModeEnabled = directModeEnabled;
        mProject = project;
        mLogStore = logStore;
        mAccessId = accessId;
        mAccessKey = accessKey;
        mCursorPosition = cursorPosition;
        mDataFetchIntervalMillis = DEFAULT_DATA_FETCH_INTERVAL_MS;
        mHeartBeatIntervalMillis = heartBeatIntervalMillis;
        mConsumeInOrder = consumeInOrder;
    }

    @Deprecated
    public LogHubConfig(String consumerGroupName, String consumerName, String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        LogHubCursorPosition cursorPosition,
                        long heartBeatIntervalMillis,
                        boolean consumeInOrder) {
        this(consumerGroupName, consumerName, endpoint, project, logStore, accessId, accessKey,
                cursorPosition, heartBeatIntervalMillis, consumeInOrder, false);
    }

    @Deprecated
    public LogHubConfig(String consumerGroupName, String consumerName, String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        int start_time,
                        long heartBeatIntervalMillis,
                        boolean consumeInOrder) {
        mConsumerGroupName = consumerGroupName;
        this.consumerName = consumerName;
        this.endpoint = endpoint;
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
    public LogHubConfig(String consumerGroupName, String consumerName, String endpoint,
                        String project, String logStore,
                        String accessId, String accessKey,
                        LogHubCursorPosition cursorPosition,
                        long heartBeatIntervalMillis,
                        boolean consumeInOrder, String stsToken) {
        this(consumerGroupName, consumerName, endpoint, project, logStore, accessId, accessKey, cursorPosition, heartBeatIntervalMillis, consumeInOrder);
        this.mStsToken = stsToken;
    }

    @Deprecated
    public LogHubConfig(String consumerGroupName,
                        String consumerName,
                        String endpoint,
                        String project,
                        String logStore,
                        String accessId,
                        String accessKey,
                        int start_time,
                        long heartBeatIntervalMillis,
                        boolean consumeInOrder,
                        String stsToken) {
        this(consumerGroupName, consumerName, endpoint, project, logStore, accessId, accessKey, start_time, heartBeatIntervalMillis, consumeInOrder);
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

    public void setConsumeInOrder(boolean order) {
        mConsumeInOrder = order;
    }

    public long getHeartBeatIntervalMillis() {
        return mHeartBeatIntervalMillis;
    }

    public void setHeartBeatIntervalMillis(long heartBeatIntervalMillis) {
        this.mHeartBeatIntervalMillis = heartBeatIntervalMillis;
    }

    public String getConsumerGroupName() {
        return mConsumerGroupName;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProject() {
        return mProject;
    }

    public String getLogStore() {
        return mLogStore;
    }

    public String getAccessId() {
        return mAccessId;
    }

    public String getAccessKey() {
        return mAccessKey;
    }

    public LogHubCursorPosition getCursorPosition() {
        return mCursorPosition;
    }

    public int GetCursorStartTime() {
        return mLoghubCursorStartTime;
    }

    public void setDirectModeEnabled(boolean directModeEnabled) {
        this.directModeEnabled = directModeEnabled;
    }

    public boolean isDirectModeEnabled() {
        return directModeEnabled;
    }

    public int getMaxFetchLogGroupSize() {
        return mMaxFetchLogGroupSize;
    }

    public void setMaxFetchLogGroupSize(int maxFetchLogGroupSize) {
        this.mMaxFetchLogGroupSize = maxFetchLogGroupSize;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }
}

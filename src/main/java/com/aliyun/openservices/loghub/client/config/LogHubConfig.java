package com.aliyun.openservices.loghub.client.config;

import java.io.Serializable;

import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.http.signer.SignVersion;

public class LogHubConfig implements Serializable {
    private static final long serialVersionUID = -460559812263406428L;

    public enum ConsumePosition {
        BEGIN_CURSOR,
        END_CURSOR
    }

    private static final long DEFAULT_FETCH_INTERVAL_MS = 200;
    // flush the check point every 60 seconds by default
    private static final long DEFAULT_COMMIT_INTERVAL_MS = 60 * 1000L;
    private static final long DEFAULT_HEARTBEAT_INTERVAL = 5000;
    private static final int DEFAULT_TIMEOUT_SEC = 60;
    private static final int DEFAULT_BATCH_SIZE = 1000;

    private String consumerGroup;
    private String consumer;
    private String endpoint;
    private String project;
    private String logstore;
    private String query;
    private String accessId;
    private String accessKey;
    private CredentialsProvider credentialsProvider;
    private LogHubCursorPosition initialPosition;
    private int startTimestamp = 0;
    private long fetchIntervalMillis = DEFAULT_FETCH_INTERVAL_MS;
    private long heartbeatIntervalMillis = DEFAULT_HEARTBEAT_INTERVAL;
    private boolean consumeInOrder = false;
    private String stsToken = null;
    private boolean directModeEnabled = false;
    private boolean autoCommitEnabled = true;
    private boolean unloadAfterCommitEnabled = false;
    private long autoCommitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int timeoutInSeconds = DEFAULT_TIMEOUT_SEC;
    private int maxInProgressingDataSizeInMB = 0;
    private String userAgent;
    private String proxyHost;
    private int proxyPort;
    private String proxyUsername;
    private String proxyPassword;
    private String proxyDomain;
    private String proxyWorkstation;

    private SignVersion signVersion = SignVersion.V1;
    private String region;

    private LogHubConfig(String consumerGroup,
                         String consumer,
                         String endpoint,
                         String project,
                         String logstore,
                         String accessId,
                         String accessKey) {
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
        this.endpoint = endpoint;
        this.project = project;
        this.logstore = logstore;
        this.accessId = accessId;
        this.accessKey = accessKey;
    }

    public LogHubConfig(String consumerGroup, String consumer, String endpoint, String project, String logstore, CredentialsProvider credentialsProvider, ConsumePosition position) {
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
        this.endpoint = endpoint;
        this.project = project;
        this.logstore = logstore;
        this.credentialsProvider = credentialsProvider;
        this.initialPosition = convertPosition(position);
    }

    public LogHubConfig(String consumerGroup, String consumer, String endpoint, String project, String logstore, CredentialsProvider credentialsProvider, int startTimestamp) {
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
        this.endpoint = endpoint;
        this.project = project;
        this.logstore = logstore;
        this.credentialsProvider = credentialsProvider;
        this.initialPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
        this.startTimestamp = startTimestamp;
    }
    
    public LogHubConfig(String consumerGroup,
                        String consumer,
                        String endpoint,
                        String project,
                        String logstore,
                        String accessId,
                        String accessKey,
                        ConsumePosition position) {
        this(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey);
        this.initialPosition = convertPosition(position);
    }

    public LogHubConfig(String consumerGroup,
                        String consumer,
                        String endpoint,
                        String project,
                        String logstore,
                        String accessId,
                        String accessKey,
                        ConsumePosition position,
                        String query) {
        this(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey, position);
        this.query = query;
    }

    private static LogHubCursorPosition convertPosition(ConsumePosition position) {
        switch (position) {
            case BEGIN_CURSOR:
                return LogHubCursorPosition.BEGIN_CURSOR;
            case END_CURSOR:
                return LogHubCursorPosition.END_CURSOR;
            default:
                throw new IllegalArgumentException("Invalid initial position: " + position);
        }
    }

    public LogHubConfig(String consumerGroup,
                        String consumer,
                        String endpoint,
                        String project,
                        String logstore,
                        String accessId,
                        String accessKey,
                        int startTimestamp) {
        this(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey);
        this.initialPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
        this.startTimestamp = startTimestamp;
    }

    public LogHubConfig(String consumerGroup, String consumer, String endpoint,
                        String project, String logstore,
                        String accessId, String accessKey,
                        ConsumePosition position,
                        int batchSize) {
        this(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey);
        this.initialPosition = convertPosition(position);
        this.batchSize = batchSize;
    }

    public LogHubConfig(String consumerGroup, String consumer, String endpoint,
                        String project, String logstore,
                        String accessId, String accessKey,
                        int startTimestamp,
                        int batchSize) {
        this(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey);
        this.initialPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
        this.startTimestamp = startTimestamp;
        this.batchSize = batchSize;
    }

    @Deprecated
    public LogHubConfig(String consumerGroup, String consumer, String endpoint,
                        String project, String logstore,
                        String accessId, String accessKey,
                        int startTime,
                        long heartBeatIntervalMillis,
                        boolean consumeInOrder) {
        this(consumerGroup, consumer, endpoint, project, logstore, accessId, accessKey);
        this.initialPosition = LogHubCursorPosition.SPECIAL_TIMER_CURSOR;
        this.startTimestamp = startTime;
        this.heartbeatIntervalMillis = heartBeatIntervalMillis;
        this.consumeInOrder = consumeInOrder;
    }

    @Deprecated
    public LogHubConfig(String consumerGroup,
                        String consumer,
                        String endpoint,
                        String project,
                        String logStore,
                        String accessId,
                        String accessKey,
                        int startTime,
                        long heartBeatIntervalMillis,
                        boolean consumeInOrder,
                        String stsToken) {
        this(consumerGroup, consumer, endpoint, project, logStore, accessId, accessKey, startTime, heartBeatIntervalMillis, consumeInOrder);
        this.stsToken = stsToken;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public String getProxyDomain() {
        return proxyDomain;
    }

    public void setProxyDomain(String proxyDomain) {
        this.proxyDomain = proxyDomain;
    }

    public String getProxyWorkstation() {
        return proxyWorkstation;
    }

    public void setProxyWorkstation(String proxyWorkstation) {
        this.proxyWorkstation = proxyWorkstation;
    }

    public String getStsToken() {
        return stsToken;
    }

    public void setStsToken(String stsToken) {
        this.stsToken = stsToken;
    }

    public long getFetchIntervalMillis() {
        return fetchIntervalMillis;
    }

    public void setFetchIntervalMillis(long fetchIntervalMillis) {
        this.fetchIntervalMillis = fetchIntervalMillis;
    }

    public boolean isConsumeInOrder() {
        return consumeInOrder;
    }

    public void setConsumeInOrder(boolean order) {
        consumeInOrder = order;
    }

    public long getHeartBeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public void setHeartBeatIntervalMillis(long heartBeatIntervalMillis) {
        this.heartbeatIntervalMillis = heartBeatIntervalMillis;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getConsumer() {
        return consumer;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProject() {
        return project;
    }

    public String getLogStore() {
        return logstore;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public LogHubCursorPosition getCursorPosition() {
        return initialPosition;
    }

    public int GetCursorStartTime() {
        return startTimestamp;
    }

    public void setDirectModeEnabled(boolean directModeEnabled) {
        this.directModeEnabled = directModeEnabled;
    }

    public boolean isDirectModeEnabled() {
        return directModeEnabled;
    }

    public int getMaxFetchLogGroupSize() {
        return batchSize;
    }

    public void setMaxFetchLogGroupSize(int maxFetchLogGroupSize) {
        this.batchSize = maxFetchLogGroupSize;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public boolean isAutoCommitEnabled() {
        return autoCommitEnabled;
    }

    public void setAutoCommitEnabled(boolean autoCommitEnabled) {
        this.autoCommitEnabled = autoCommitEnabled;
    }

    public long getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(long autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public boolean isUnloadAfterCommitEnabled() {
        return unloadAfterCommitEnabled;
    }

    public void setUnloadAfterCommitEnabled(boolean unloadAfterCommitEnabled) {
        this.unloadAfterCommitEnabled = unloadAfterCommitEnabled;
    }

    public int getTimeoutInSeconds() {
        return timeoutInSeconds;
    }

    public void setTimeoutInSeconds(int timeoutInSeconds) {
        this.timeoutInSeconds = timeoutInSeconds;
    }

    public int getMaxInProgressingDataSizeInMB() {
        return maxInProgressingDataSizeInMB;
    }

    public void setMaxInProgressingDataSizeInMB(int maxInProgressingDataSizeInMB) {
        this.maxInProgressingDataSizeInMB = maxInProgressingDataSizeInMB;
    }

    public CredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    public boolean hasQuery() {
        return query != null && !query.isEmpty();
    }
    public SignVersion getSignVersion() {
        return signVersion;
    }

    public void setSignVersion(SignVersion signVersion) {
        this.signVersion = signVersion;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "LogHubConfig{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", consumer='" + consumer + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", project='" + project + '\'' +
                ", logstore='" + logstore + '\'' +
                ", accessId='" + accessId + '\'' +
                ", initialPosition=" + initialPosition +
                ", startTimestamp=" + startTimestamp +
                ", fetchIntervalMillis=" + fetchIntervalMillis +
                ", heartbeatIntervalMillis=" + heartbeatIntervalMillis +
                ", consumeInOrder=" + consumeInOrder +
                ", directModeEnabled=" + directModeEnabled +
                ", autoCommitEnabled=" + autoCommitEnabled +
                ", unloadAfterCommitEnabled=" + unloadAfterCommitEnabled +
                ", autoCommitIntervalMs=" + autoCommitIntervalMs +
                ", batchSize=" + batchSize +
                ", timeoutInSeconds=" + timeoutInSeconds +
                ", maxInProgressingDataSizeInMB=" + maxInProgressingDataSizeInMB +
                ", userAgent='" + userAgent + '\'' +
                '}';
    }
}

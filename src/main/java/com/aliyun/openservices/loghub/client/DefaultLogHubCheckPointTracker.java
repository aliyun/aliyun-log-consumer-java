package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class DefaultLogHubCheckPointTracker implements ILogHubCheckPointTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogHubCheckPointTracker.class);

    private LogHubClientAdapter loghubClient;
    private String cursor;
    private String latestCursor;
    private String pendingCheckpoint = "";
    private String lastSavedCheckpoint = "";
    private final String consumer;
    private final int shardID;
    private final long autoCommitInterval;
    private final boolean autoCommitEnabled;
    private volatile boolean allCommitted;
    private long lastCheckTimeInMillis;
    private LogHubHeartBeat heartBeat;

    public DefaultLogHubCheckPointTracker(LogHubClientAdapter loghubClient,
                                          LogHubConfig config,
                                          LogHubHeartBeat heartBeat,
                                          int shardId) {
        this.loghubClient = loghubClient;
        this.consumer = config.getConsumer();
        this.shardID = shardId;
        this.lastCheckTimeInMillis = System.currentTimeMillis();
        this.autoCommitEnabled = config.isAutoCommitEnabled();
        this.autoCommitInterval = config.getAutoCommitIntervalMs();
        this.allCommitted = true;
        this.heartBeat = heartBeat;
    }

    /**
     * Sets the latest cursor. Called in process stage only.
     */
    public void setCursor(String cursor) {
        this.cursor = cursor;
        if ((cursor == null && latestCursor != null) || (cursor != null && !cursor.equals(latestCursor))) {
            this.latestCursor = cursor;
            this.allCommitted = false;
        }
    }

    public String getCursor() {
        return cursor;
    }

    public void saveCheckPoint(boolean persistent) throws LogHubCheckPointException {
        pendingCheckpoint = cursor;
        if (persistent) {
            flushCheckpoint();
        }
    }

    public void setInPersistentCheckPoint(String cursor) {
        lastSavedCheckpoint = cursor;
    }

    public void saveCheckPoint(String cursor, boolean persistent) throws LogHubCheckPointException {
        pendingCheckpoint = cursor;
        if (persistent) {
            flushCheckpoint();
        }
    }

    public void flushCheckpoint() throws LogHubCheckPointException {
        String checkpoint = pendingCheckpoint;
        if (checkpoint == null || checkpoint.isEmpty() || checkpoint.equals(lastSavedCheckpoint)) {
            return;
        }
        for (int i = 0; ; i++) {
            try {
                loghubClient.UpdateCheckPoint(shardID, consumer, checkpoint);
                lastSavedCheckpoint = checkpoint;
                break;
            } catch (LogException e) {
                final String errorCode = e.GetErrorCode();
                if ("ConsumerNotExist".equalsIgnoreCase(errorCode)
                        || "ConsumerNotMatch".equalsIgnoreCase(errorCode)) {
                    heartBeat.markIdle(shardID);
                    LOG.warn("Consumer {} has been removed or shard has been reassigned - {}", consumer, e.GetErrorMessage());
                    break;
                }
                if (i >= 2) {
                    throw new LogHubCheckPointException(
                            "Failed to save checkpoint, " + consumer + ", " + shardID + ", " + checkpoint, e);
                }
                LoghubClientUtil.sleep(new Random().nextInt(200));
            }
        }
        if (latestCursor == null || latestCursor.equals(lastSavedCheckpoint)) {
            allCommitted = true;
        }
    }

    void flushCheckpointIfNeeded() {
        if (!autoCommitEnabled) {
            return;
        }
        long curTime = System.currentTimeMillis();
        if (curTime > lastCheckTimeInMillis + autoCommitInterval) {
            try {
                flushCheckpoint();
            } catch (LogHubCheckPointException e) {
                LOG.error("Error while flushing checkpoint", e);
            }
            lastCheckTimeInMillis = curTime;
        }
    }

    @Override
    public String getCheckPoint() {
        return pendingCheckpoint;
    }

    boolean isAllCommitted() {
        return allCommitted;
    }
}

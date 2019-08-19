package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogHubCheckPointTracker implements ILogHubCheckPointTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogHubCheckPointTracker.class);

    private String mCursor;
    private String mTempCheckPoint = "";
    private String mLastPersistentCheckPoint = "";
    private LogHubClientAdapter loghubClient;
    private String consumerName;
    private int mShardId;
    private long mLastCheckTime;
    private boolean autoCommitEnabled;
    private long autoCommitInterval;

    public DefaultLogHubCheckPointTracker(LogHubClientAdapter loghubClient,
                                          LogHubConfig config,
                                          int shardId) {
        this.loghubClient = loghubClient;
        this.consumerName = config.getConsumerName();
        this.mShardId = shardId;
        this.mLastCheckTime = System.currentTimeMillis();
        this.autoCommitEnabled = config.isAutoCommitEnabled();
        this.autoCommitInterval = config.getAutoCommitIntervalMs();
    }

    public void setCursor(String cursor) {
        mCursor = cursor;
    }

    public String getCursor() {
        return mCursor;
    }

    public void saveCheckPoint(boolean persistent)
            throws LogHubCheckPointException {
        mTempCheckPoint = mCursor;
        if (persistent) {
            flushCheckPoint();
        }
    }

    public void setInMemoryCheckPoint(String cursor) {
        mTempCheckPoint = cursor;
    }

    public void setInPersistentCheckPoint(String cursor) {
        mLastPersistentCheckPoint = cursor;
    }

    public void saveCheckPoint(String cursor, boolean persistent)
            throws LogHubCheckPointException {
        mTempCheckPoint = cursor;
        if (persistent) {
            flushCheckPoint();
        }
    }

    public void flushCheckPoint() throws LogHubCheckPointException {
        String toPersistent = mTempCheckPoint;
        if (toPersistent != null && !toPersistent.equals(mLastPersistentCheckPoint)) {
            try {
                loghubClient.UpdateCheckPoint(mShardId, consumerName, toPersistent);
                mLastPersistentCheckPoint = toPersistent;
            } catch (LogException e) {
                throw new LogHubCheckPointException(
                        "fail to persistent the cursor to outside system, " + consumerName + ", " + mShardId + ", " + toPersistent, e);
            }
        }
    }

    public void flushCheck() {
        if (!autoCommitEnabled) {
            return;
        }
        long curTime = System.currentTimeMillis();
        if (curTime > mLastCheckTime + autoCommitInterval) {
            try {
                flushCheckPoint();
            } catch (LogHubCheckPointException e) {
                LOG.error("Error while flushing checkpoint", e);
            }
            mLastCheckTime = curTime;
        }
    }

    @Override
    public String getCheckPoint() {
        return mTempCheckPoint;
    }
}

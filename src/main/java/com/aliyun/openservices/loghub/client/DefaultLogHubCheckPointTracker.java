package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;

public class DefaultLogHubCheckPointTracker implements ILogHubCheckPointTracker {
    private String mCursor;
    private String mTempCheckPoint = "";
    private String mLastPersistentCheckPoint = "";

    private LogHubClientAdapter mLogHubClientAdapter;
    private String consumerName;
    private int mShardId;
    private long mLastCheckTime;
    // flush the check point every 60 seconds by default
    private static long DEFAULT_FLUSH_CHECK_POINT_INTERVAL_NANOS = 60 * 1000L * 1000 * 1000L;

    public DefaultLogHubCheckPointTracker(LogHubClientAdapter logHubClientAdapter,
                                          String consumerName, int shardId) {
        mLogHubClientAdapter = logHubClientAdapter;
        this.consumerName = consumerName;
        mShardId = shardId;
        mLastCheckTime = System.nanoTime();
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
                mLogHubClientAdapter.UpdateCheckPoint(mShardId, consumerName, toPersistent);
                mLastPersistentCheckPoint = toPersistent;
            } catch (LogException e) {
                throw new LogHubCheckPointException(
                        "fail to persistent the cursor to outside system, " + consumerName + ", " + mShardId + ", " + toPersistent, e);
            }
        }
    }

    public void flushCheck() {
        long curTime = System.nanoTime();
        if (curTime > mLastCheckTime + DEFAULT_FLUSH_CHECK_POINT_INTERVAL_NANOS) {
            try {
                flushCheckPoint();
            } catch (LogHubCheckPointException e) {
            }
            mLastCheckTime = curTime;
        }
    }

    @Override
    public String getCheckPoint() {
        return mTempCheckPoint;
    }
}

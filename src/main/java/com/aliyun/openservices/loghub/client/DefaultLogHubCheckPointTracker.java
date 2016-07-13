package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;

public class DefaultLogHubCheckPointTracker implements ILogHubCheckPointTracker {
	private String mCursor;
	private String mTempCheckPoint = "";
	private String mLastPersistentCheckPoint = "";

	private LogHubClientAdapter mLogHubClientAdapter;
	private String mInstanceName;
	private int mShardId;
	private long mLastCheckTime;
	// flush the check point every 60 seconds by default
	private static long DEFAULT_FLUSH_CHECK_POINT_TERVAL_NANOS = 60 * 1000L * 1000 * 1000L;

	public DefaultLogHubCheckPointTracker(LogHubClientAdapter logHubClientAdapter,
			String instanceName, int shardId) {
		mLogHubClientAdapter = logHubClientAdapter;
		mInstanceName = instanceName;
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
	
	public void setInMemoryCheckPoint(String cursor)
	{
		mTempCheckPoint = cursor;
	}
	
	public void setInPeristentCheckPoint(String cursor)
	{
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

		if (toPersistent != null
				&& toPersistent.equals(mLastPersistentCheckPoint) == false) {
			try {
				mLogHubClientAdapter.UpdateCheckPoint(mShardId, mInstanceName, toPersistent);
				mLastPersistentCheckPoint = toPersistent;
			} catch (LogException e) {
				throw new LogHubCheckPointException(
						"Failed to persistent the cursor to outside system, " + mInstanceName + ", " + mShardId + ", " + toPersistent, e);
			}
		}
	}

	public void flushCheck() {
		long curTime = System.nanoTime();
		if (curTime > mLastCheckTime + DEFAULT_FLUSH_CHECK_POINT_TERVAL_NANOS) {
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

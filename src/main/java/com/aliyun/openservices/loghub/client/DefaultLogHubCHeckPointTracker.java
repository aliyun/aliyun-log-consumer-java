package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;

public class DefaultLogHubCHeckPointTracker implements ILogHubCheckPointTracker {
	private String mCursor;
	private String mTempCheckPoint;
	private String mLastPersistentCheckPoint;

	private ILogHubLeaseManager mLeaseManager;
	private String mInstanceName;
	private String mShardId;
	private long mLastCheckTime;
	// flush the check point every 60 seconds by default
	private static long DEFAULT_FLUSH_CHECK_POINT_TERVAL_NANOS = 60 * 1000L * 1000 * 1000L;

	public DefaultLogHubCHeckPointTracker(ILogHubLeaseManager leaseManager,
			String instanceName, String shardId) {
		mLeaseManager = leaseManager;
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
				if (mLeaseManager.updateCheckPoint(mShardId, toPersistent,
						mInstanceName)) {
					mLastPersistentCheckPoint = toPersistent;
				}
			} catch (LogHubLeaseException e) {
				throw new LogHubCheckPointException(
						"Failed to persistent the cursor to outside system", e);
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
}

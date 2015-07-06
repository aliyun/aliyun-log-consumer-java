package com.aliyun.openservices.loghub.client.sample;

import java.util.List;

import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.sls.common.LogGroupData;
import com.aliyun.openservices.sls.common.LogItem;

public class SampleLogHubProcessor implements ILogHubProcessor {

	private String mShardId;
	private long mLastCheckTime = 0;
	@Override
	public void initialize(String shardId) {
		mShardId = shardId;
		
	}

	@Override
	public void process(List<LogGroupData> logGroups,
			ILogHubCheckPointTracker checkPointTracker) {
		
		for (LogGroupData group : logGroups) {
			List<LogItem> items = group.GetAllLogs();
		
			for (LogItem item : items) {
				System.out.println("shard_id:" + mShardId + " " + item.ToJsonString());
			}
		}
		long curTime = System.currentTimeMillis();
		if (curTime - mLastCheckTime > 1000 * 3) {
			try {
				checkPointTracker.saveCheckPoint(true);
			} catch (LogHubCheckPointException e) {
				
				e.printStackTrace();
			}
			mLastCheckTime = curTime;
		} else {
			try {
				checkPointTracker.saveCheckPoint(false);
			} catch (LogHubCheckPointException e) {
				
				e.printStackTrace();
			}
		}
	}

	@Override
	public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
		
		
	}
	

}

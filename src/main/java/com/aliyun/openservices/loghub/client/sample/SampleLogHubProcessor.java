package com.aliyun.openservices.loghub.client.sample;

import java.util.List;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.client.DefaultLogHubCHeckPointTracker;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.sls.common.LogGroupData;
import com.aliyun.openservices.sls.common.LogItem;

public class SampleLogHubProcessor implements ILogHubProcessor {

	private String mShardId;
	private long mLastCheckTime = 0;
	private static final Logger logger = Logger.getLogger(SampleLogHubProcessor.class);
	@Override
	public void initialize(String shardId) {
		mShardId = shardId;
		
	}

	@Override
	public void process(List<LogGroupData> logGroups,
			ILogHubCheckPointTracker checkPointTracker) {
		DefaultLogHubCHeckPointTracker tracher = (DefaultLogHubCHeckPointTracker)(checkPointTracker);
		logger.info("process data, shard id = " + mShardId + " next cursor" + tracher.getCursor() + " size:" + logGroups.size());
		for (LogGroupData group : logGroups) {
			List<LogItem> items = group.GetAllLogs();
		
			if (items.size() > 0)
			{
				logger.info("shard_id:" + mShardId + " " + items.get(0).mLogTime);;
				break;
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

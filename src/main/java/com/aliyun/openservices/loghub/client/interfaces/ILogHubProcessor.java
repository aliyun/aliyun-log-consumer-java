package com.aliyun.openservices.loghub.client.interfaces;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;

import java.util.List;

public interface ILogHubProcessor {

	void initialize(int shardId);

	/**
	 * Process the data, and roll back if the return value is not NULL ""
	 * 
	 * @param logGroups the loggroups to process 
	 * @param checkPointTracker the check point tracker
	 * @return the roll backed check point. if return NULL or "", the consumer
	 *         will read log data ahead, wise other, roll back the shard to the
	 *         returned check point(shard cursor will roll back)
	 */
	String process(List<LogGroupData> logGroups,
	               ILogHubCheckPointTracker checkPointTracker);

	void shutdown(ILogHubCheckPointTracker checkPointTracker);
}

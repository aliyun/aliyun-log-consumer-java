package com.aliyun.openservices.loghub.client.interfaces;

import java.util.List;

import com.aliyun.openservices.sls.common.LogGroupData;

import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;

public interface ILogHubProcessor {

	 public void initialize(String shardId);
	 
	 public void process(List<LogGroupData> logGroups, ILogHubCheckPointTracker checkPointTracker);
	 
	 public void shutdown(ILogHubCheckPointTracker checkPointTracker);
}

package com.aliyun.openservices.loghub.client.interfaces;

import java.util.List;

import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.common.LogGroup;

public interface ILogHubProcessor {

	 public void initialize(String shardId);
	 
	 public void process(List<LogGroup> logGroups, ILogHubCheckPointTracker checkPointTracker);
	 
	 public void shutdown(ILogHubCheckPointTracker checkPointTracker);
}

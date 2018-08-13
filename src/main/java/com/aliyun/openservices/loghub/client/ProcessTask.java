package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;

public class ProcessTask implements ITask {

	private ILogHubProcessor mProcessor;
	private List<LogGroupData> mLogGroup;
	private DefaultLogHubCheckPointTracker mCheckPointTracker;

	public ProcessTask(ILogHubProcessor processor, List<LogGroupData> logGroups,
			DefaultLogHubCheckPointTracker checkPointTracker) {
		mProcessor = processor;
		mLogGroup = logGroups;
		mCheckPointTracker = checkPointTracker;

	}

	public TaskResult call() {
		String checkpoint = null;
		try {
			checkpoint = mProcessor.process(mLogGroup, mCheckPointTracker);
			mCheckPointTracker.flushCheck();
		} catch (Exception e) {
			return new TaskResult(e);
		}
		return new ProcessTaskResult(checkpoint);
	}
}

package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.sls.common.LogGroupData;

public class ProcessTask implements ITask {

	private ILogHubProcessor mProcessor;
	private List<LogGroupData> mLogGroup;
	private DefaultLogHubCHeckPointTracker mCheckPointTracker;

	public ProcessTask(ILogHubProcessor processor, List<LogGroupData> logGroups,
			DefaultLogHubCHeckPointTracker checkPointTracker) {
		mProcessor = processor;
		mLogGroup = logGroups;
		mCheckPointTracker = checkPointTracker;

	}

	public TaskResult call() {
		try {
			mProcessor.process(mLogGroup, mCheckPointTracker);
			mCheckPointTracker.flushCheck();
		} catch (Exception e) {
			e.printStackTrace();
			return new TaskResult(e);
		}
		return new TaskResult(null);
	}
}

package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.common.LogGroup;

public class ProcessTask implements ITask {

	private ILogHubProcessor mProcessor;
	private List<LogGroup> mLogGroup;
	private DefaultLogHubCHeckPointTracker mCheckPointTracker;

	public ProcessTask(ILogHubProcessor processor, List<LogGroup> logGroups,
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

	public TaskType getTaskType() {
		return null;
	}

}

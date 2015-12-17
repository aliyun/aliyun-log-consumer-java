package com.aliyun.openservices.loghub.client;


import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;

public class ShutDownTask implements ITask {

	private ILogHubProcessor mProcessor;
	private DefaultLogHubCheckPointTracker mCheckPointTracker;
	private static final Logger logger = Logger.getLogger(ShutDownTask.class);

	public ShutDownTask(ILogHubProcessor processor,
			DefaultLogHubCheckPointTracker checkPointTracker) {
		mProcessor = processor;
		mCheckPointTracker = checkPointTracker;
	}

	public TaskResult call() {

		Exception exception = null;
		try {
			mProcessor.shutdown(mCheckPointTracker);
		} catch (Exception e) {
			exception = null;
		}
		try {
			mCheckPointTracker.flushCheckPoint();
		} catch (Exception e) {
			logger.error("Failed to flush check point", e);
		}

		return new TaskResult(exception);
	}

}

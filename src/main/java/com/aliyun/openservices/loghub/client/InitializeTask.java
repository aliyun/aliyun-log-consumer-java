package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.LogHubClient;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;
import com.aliyun.openservices.loghub.common.LogHubModes.LogHubMode;
import com.aliyun.openservices.loghub.response.GetCursorResponse;

public class InitializeTask implements ITask {

	private ILogHubLeaseManager mLeaseManager;
	private ILogHubProcessor mProcessor;
	private LogHubClient mLogHubClient;
	private String mProject;
	private String mLogStream;
	private String mShardId;
	private LogHubCursorPosition mCursorPosition;

	public InitializeTask(ILogHubProcessor processor,
			ILogHubLeaseManager leaseManager, LogHubClient logHubClient,
			String project, String logStream, String shardId, LogHubCursorPosition cursorPosition) {
		mProcessor = processor;
		mLeaseManager = leaseManager;
		mLogHubClient = logHubClient;
		mProject = project;
		mLogStream = logStream;
		mShardId = shardId;
		mCursorPosition = cursorPosition;
	}

	public TaskResult call() {
		try {
			mProcessor.initialize(mShardId);
			String checkPoint = mLeaseManager.getCheckPoint(mShardId);
			String cursor = null;
			LogHubMode mode = null;
			if (checkPoint != null && checkPoint.length() > 0) {
				cursor = checkPoint;
				mode = LogHubMode.AFTER;
			} else {
				// get cursor from loghub client , begin or end
				GetCursorResponse cursorResponse = null;
				if(mCursorPosition.equals(LogHubCursorPosition.BEGIN_CURSOR))
				{
					cursorResponse = mLogHubClient.getCursor(mProject, mLogStream, Integer.parseInt(mShardId), LogHubMode.BEGIN);
					cursor = cursorResponse.getCursor();
					mode = LogHubMode.AT;
				}
				else
				{
					cursorResponse = mLogHubClient.getCursor(mProject, mLogStream, Integer.parseInt(mShardId), LogHubMode.END);	
					cursor = cursorResponse.getCursor();
					mode = LogHubMode.AFTER;
				}
			}
			return new InitTaskResult(cursor, mode);
		} catch (Exception e) {
			return new TaskResult(e);
		}
	}
}

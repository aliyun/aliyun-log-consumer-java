package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.sls.response.GetCursorResponse;
import com.aliyun.openservices.sls.common.SlsConsts.CursorMode;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;

public class InitializeTask implements ITask {

	private ILogHubLeaseManager mLeaseManager;
	private ILogHubProcessor mProcessor;
	private SLSClient mLogHubClient;
	private String mProject;
	private String mLogStream;
	private String mShardId;
	private LogHubCursorPosition mCursorPosition;
	private long mCursorStartTime = 0;

	public InitializeTask(ILogHubProcessor processor,
			ILogHubLeaseManager leaseManager, SLSClient logHubClient,
			String project, String logStream, String shardId,
			LogHubCursorPosition cursorPosition, long cursorStartTime) {
		mProcessor = processor;
		mLeaseManager = leaseManager;
		mLogHubClient = logHubClient;
		mProject = project;
		mLogStream = logStream;
		mShardId = shardId;
		mCursorPosition = cursorPosition;
		mCursorStartTime = cursorStartTime;
	}

	public TaskResult call() {
		try {
			mProcessor.initialize(mShardId);
			String checkPoint = mLeaseManager.getCheckPoint(mShardId);
			String cursor = null;
			if (checkPoint != null && checkPoint.length() > 0) {
				cursor = checkPoint;
			} else {
				// get cursor from loghub client , begin or end
				GetCursorResponse cursorResponse = null;
				if(mCursorPosition.equals(LogHubCursorPosition.BEGIN_CURSOR))
				{
					cursorResponse = mLogHubClient.GetCursor(mProject, mLogStream, Integer.parseInt(mShardId), CursorMode.BEGIN);
					cursor = cursorResponse.GetCursor();
				}
				else if (mCursorPosition.equals(LogHubCursorPosition.END_CURSOR))
				{
					cursorResponse = mLogHubClient.GetCursor(mProject, mLogStream, Integer.parseInt(mShardId), CursorMode.END);	
					cursor = cursorResponse.GetCursor();
				}
				else
				{
					cursorResponse = mLogHubClient.GetCursor(mProject, mLogStream, Integer.parseInt(mShardId), mCursorStartTime);	
					cursor = cursorResponse.GetCursor();
				}
			}
			return new InitTaskResult(cursor);
		} catch (Exception e) {
			return new TaskResult(e);
		}
	}
}

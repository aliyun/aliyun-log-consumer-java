package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;

public class InitializeTask implements ITask {

	private LogHubClientAdapter mLogHubClientAdapter;
	private ILogHubProcessor mProcessor;
	private int mShardId;
	private LogHubCursorPosition mCursorPosition;
	private long mCursorStartTime = 0;

	public InitializeTask(ILogHubProcessor processor, LogHubClientAdapter logHubClientAdapter,
			int shardId,
			LogHubCursorPosition cursorPosition, long cursorStartTime) {
		mProcessor = processor;
		mLogHubClientAdapter = logHubClientAdapter;
		mShardId = shardId;
		mCursorPosition = cursorPosition;
		mCursorStartTime = cursorStartTime;
	}

	public TaskResult call() {
		try {
			mProcessor.initialize(mShardId);
			boolean is_cursor_persistent = false;
			String checkPoint = mLogHubClientAdapter.GetCheckPoint(mShardId);
			String cursor = null;
			if (checkPoint != null && checkPoint.length() > 0) {
				is_cursor_persistent = true;
				cursor = checkPoint;
			} else {
				// get cursor from loghub client , begin or end
				if(mCursorPosition.equals(LogHubCursorPosition.BEGIN_CURSOR))
				{
					cursor = mLogHubClientAdapter.GetCursor(mShardId, CursorMode.BEGIN);
				}
				else if (mCursorPosition.equals(LogHubCursorPosition.END_CURSOR))
				{
					cursor = mLogHubClientAdapter.GetCursor(mShardId, CursorMode.END);
				}
				else
				{
					cursor = mLogHubClientAdapter.GetCursor(mShardId, mCursorStartTime);
				}
			}
			return new InitTaskResult(cursor, is_cursor_persistent);
		} catch (Exception e) {
			return new TaskResult(e);
		}
	}
}

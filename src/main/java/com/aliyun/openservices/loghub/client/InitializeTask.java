package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitializeTask implements ITask {
    private static final Logger LOG = LoggerFactory.getLogger(InitializeTask.class);

    private LogHubClientAdapter mLogHubClientAdapter;
    private ILogHubProcessor mProcessor;
    private int mShardId;
    private LogHubCursorPosition mCursorPosition;
    private long mCursorStartTime;

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
            boolean isCursorPersistent = false;
            String checkPoint = mLogHubClientAdapter.GetCheckPoint(mShardId);
            String cursor = null;
            if (checkPoint != null && checkPoint.length() > 0) {
                isCursorPersistent = true;
                cursor = checkPoint;
            } else {
                // get cursor from loghub client , begin or end
                if (mCursorPosition.equals(LogHubCursorPosition.BEGIN_CURSOR)) {
                    cursor = mLogHubClientAdapter.GetCursor(mShardId, CursorMode.BEGIN);
                } else if (mCursorPosition.equals(LogHubCursorPosition.END_CURSOR)) {
                    cursor = mLogHubClientAdapter.GetCursor(mShardId, CursorMode.END);
                } else {
                    cursor = mLogHubClientAdapter.GetCursor(mShardId, mCursorStartTime);
                }
            }
            return new InitTaskResult(cursor, isCursorPersistent);
        } catch (Exception e) {
            LOG.error("Error fetching initial position", e);
            return new TaskResult(e);
        }
    }
}

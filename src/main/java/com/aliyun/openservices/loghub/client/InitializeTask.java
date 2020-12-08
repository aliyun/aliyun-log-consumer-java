package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitializeTask implements ITask {
    private static final Logger LOG = LoggerFactory.getLogger(InitializeTask.class);

    private LogHubClientAdapter loghubClient;
    private ILogHubProcessor processor;
    private int shardId;
    private LogHubCursorPosition position;
    private long startTime;

    public InitializeTask(ILogHubProcessor processor,
                          LogHubClientAdapter loghubClient,
                          int shardId,
                          LogHubCursorPosition position,
                          long startTime) {
        this.processor = processor;
        this.loghubClient = loghubClient;
        this.shardId = shardId;
        this.position = position;
        this.startTime = startTime;
    }

    public TaskResult call() {
        try {
            processor.initialize(shardId);
            boolean isCursorPersistent = false;
            String checkPoint = loghubClient.GetCheckPoint(shardId);
            String cursor = null;
            if (checkPoint != null && checkPoint.length() > 0) {
                isCursorPersistent = true;
                cursor = checkPoint;
            } else {
                // get cursor from loghub client , begin or end
                cursor = loghubClient.getCursor(shardId, position, startTime);
            }
            return new InitTaskResult(cursor, isCursorPersistent);
        } catch (Exception e) {
            LOG.error("Error fetching initial position", e);
            return new TaskResult(e);
        }
    }
}

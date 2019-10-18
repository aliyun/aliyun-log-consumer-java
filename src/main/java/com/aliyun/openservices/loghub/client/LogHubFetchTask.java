package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LogHubFetchTask implements ITask {
    private LogHubClientAdapter loghubClient;
    private String cursor;
    private int shardId;
    private int maxFetchLogGroupSize;
    private static final Logger LOG = LoggerFactory.getLogger(LogHubFetchTask.class);

    public LogHubFetchTask(LogHubClientAdapter loghubClient, int shardId, String cursor, int maxFetchLogGroupSize) {
        this.loghubClient = loghubClient;
        this.shardId = shardId;
        this.cursor = cursor;
        this.maxFetchLogGroupSize = maxFetchLogGroupSize;
    }

    public TaskResult call() {
        Exception exception = null;
        boolean retry = false;
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                BatchGetLogResponse response = loghubClient.BatchGetLogs(
                        shardId, maxFetchLogGroupSize, cursor);
                List<LogGroupData> fetchedData = response.GetLogGroups();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("shard {}, cursor {}, next cursor {}, response size: {}", shardId, cursor,
                            response.GetNextCursor(), response.GetCount());
                }
                String nextCursor = response.GetNextCursor();
                if (nextCursor.isEmpty()) {
                    nextCursor = cursor;
                }
                return new FetchTaskResult(fetchedData, nextCursor, response.GetRawSize());
            } catch (LogException lex) {
                if (attempt == 0 && lex.GetErrorCode().toLowerCase().contains("invalidcursor")) {
                    retry = true;
                }
                exception = lex;
            } catch (Exception e) {
                exception = e;
            }
            if (retry) {
                // TODO Why we need this?
                try {
                    refreshCursor();
                } catch (Exception e) {
                    LOG.warn("Could not refresh cursor", e);
                    break;
                }
                retry = false;
            }
        }
        return new TaskResult(exception);
    }

    private void refreshCursor() throws LogException {
        cursor = loghubClient.GetCursor(shardId, CursorMode.END);
    }
}

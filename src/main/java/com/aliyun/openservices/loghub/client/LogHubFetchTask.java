package com.aliyun.openservices.loghub.client;

import java.util.List;

import org.apache.log4j.Logger;

import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;

public class LogHubFetchTask implements ITask {
    private LogHubClientAdapter logHubClientAdapter;
    private int shardId;
    private String cursor;
    private int maxFetchLogGroupSize;
    private static final Logger LOG = Logger.getLogger(LogHubFetchTask.class);

    public LogHubFetchTask(LogHubClientAdapter logHubClientAdapter, int shardId, String cursor, int maxFetchLogGroupSize) {
        this.logHubClientAdapter = logHubClientAdapter;
        this.shardId = shardId;
        this.cursor = cursor;
        this.maxFetchLogGroupSize = maxFetchLogGroupSize;
    }

    public TaskResult call() {
        Exception exception = null;
        boolean retry = false;
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                BatchGetLogResponse response = logHubClientAdapter.BatchGetLogs(
                        shardId, maxFetchLogGroupSize, cursor);
                List<LogGroupData> fetchedData = response.GetLogGroups();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("shard id = " + shardId + " cursor = " + cursor
                            + " next cursor" + response.GetNextCursor() + " size:"
                            + response.GetCount());
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
        cursor = logHubClientAdapter.GetCursor(shardId, CursorMode.END);
    }
}

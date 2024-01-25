package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.PullLogsResponse;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LogHubFetchTask implements ITask {
    private static final Logger LOG = LoggerFactory.getLogger(LogHubFetchTask.class);

    private LogHubClientAdapter loghubClient;
    private String cursor;
    private int shardId;
    private LogHubConfig config;

    public LogHubFetchTask(LogHubClientAdapter loghubClient,
                           int shardId,
                           String cursor,
                           LogHubConfig config) {
        this.loghubClient = loghubClient;
        this.shardId = shardId;
        this.cursor = cursor;
        this.config = config;
    }

    public TaskResult call() {
        Exception exception = null;
        for (int attempt = 0; ; attempt++) {
            try {
                PullLogsResponse response = loghubClient.PullLogs(
                        shardId, cursor, config);
                List<LogGroupData> fetchedData = response.getLogGroups();
                LOG.debug("shard {}, cursor {}, next cursor {}, response size: {}", shardId, cursor,
                        response.getNextCursor(), response.getCount());
                String nextCursor = response.getNextCursor();
                if (nextCursor.isEmpty()) {
                    LOG.info("Shard {} next cursor is empty, set to current cursor {}", shardId, cursor);
                    nextCursor = cursor;
                }
                long rawSizeBeforeQuery = 0;
                int rawLogGroupCountBeforeQuery = 0;
                if (config.hasQuery()) {
                    rawSizeBeforeQuery = Math.max(response.getRawDataSize(), 0);
                    rawLogGroupCountBeforeQuery = Math.max(response.getRawDataCount(), 0);
                }
                return new FetchTaskResult(fetchedData, cursor, nextCursor, response.getRawSize(), rawSizeBeforeQuery,
                        rawLogGroupCountBeforeQuery);
            } catch (LogException lex) {
                if (attempt == 0 && lex.GetErrorCode().toLowerCase().contains("invalidcursor")) {
                    // If checkpoint is invalid, such as expired cursor, will
                    // start from default position.
                    resetCursor();
                    continue;
                }
                LOG.error("Fail to pull data from shard {}, cursor {}", shardId, cursor, lex);
                if (attempt >= 1) {
                    exception = lex;
                    break;
                }
            }
            LoghubClientUtil.sleep(200);
        }
        return new TaskResult(exception);
    }

    private void resetCursor() {
        try {
            String defaultCursor = loghubClient.getCursor(shardId, config.getCursorPosition(), config.GetCursorStartTime());
            LOG.info("Invalid cursor {}, reset to default position {}", cursor, defaultCursor);
            cursor = defaultCursor;
        } catch (LogException ex) {
            LOG.error("Unable to reset cursor", ex);
        }
    }
}

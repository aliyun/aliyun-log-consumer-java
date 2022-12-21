package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.LogGroupData;

import java.util.List;

public class FetchedLogGroup {
    private final int shardId;
    private final List<LogGroupData> fetchedData;
    private final String nextCursor;
    private final String cursor;

    public FetchedLogGroup(int shardId, List<LogGroupData> fetchedData, String nextCursor, String cursor) {
        this.shardId = shardId;
        this.fetchedData = fetchedData;
        this.nextCursor = nextCursor;
        this.cursor = cursor;
    }

    public int getShardId() {
        return shardId;
    }

    public List<LogGroupData> getFetchedData() {
        return fetchedData;
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public String getCursor() {
        return cursor;
    }
}

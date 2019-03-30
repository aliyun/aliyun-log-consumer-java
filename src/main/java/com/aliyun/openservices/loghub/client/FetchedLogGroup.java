package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.LogGroupData;

import java.util.List;

public class FetchedLogGroup {
    private final int shardId;
    private final List<LogGroupData> fetchedData;
    private final String endCursor;

    public FetchedLogGroup(int shardId, List<LogGroupData> fetchedData, String endCursor) {
        this.shardId = shardId;
        this.fetchedData = fetchedData;
        this.endCursor = endCursor;
    }

    public int getShardId() {
        return shardId;
    }

    public List<LogGroupData> getFetchedData() {
        return fetchedData;
    }

    public String getEndCursor() {
        return endCursor;
    }
}

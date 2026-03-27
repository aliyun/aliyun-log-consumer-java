package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.LogGroupData;

import java.util.List;

public class FetchTaskResult extends TaskResult {

    private List<LogGroupData> fetchData;
    private String cursor;
    private String nextCursor;
    private int rawSize;
    private long rawSizeBeforeQuery;
    private int rawLogGroupCountBeforeQuery;
    private boolean endOfCursor;

    public FetchTaskResult(List<LogGroupData> fetchData, String cursor, String nextCursor, int rawSize, long rawSizeBeforeQuery, int rawLogGroupCountBeforeQuery, boolean endOfCursor) {
        super(null);
        this.fetchData = fetchData;
        this.cursor = cursor;
        this.nextCursor = nextCursor;
        this.rawSize = rawSize;
        this.rawSizeBeforeQuery = rawSizeBeforeQuery;
        this.rawLogGroupCountBeforeQuery = rawLogGroupCountBeforeQuery;
        this.endOfCursor = endOfCursor;
    }

    public List<LogGroupData> getFetchedData() {
        return fetchData;
    }

    public String getCursor() {
        return cursor;
    }

    public String getNextCursor() {
        return nextCursor;
    }

    public int getRawSize() {
        return rawSize;
    }

    public long getRawSizeBeforeQuery() {
        return rawSizeBeforeQuery;
    }
    public int getRawLogGroupCountBeforeQuery() {
        return rawLogGroupCountBeforeQuery;
    }

    public boolean isEndOfCursor() {
        return endOfCursor;
    }
}

package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.LogGroupData;

import java.util.List;

public class FetchTaskResult extends TaskResult {

    private List<LogGroupData> fetchData;
    private String cursor;
    private int rawSize;

    public FetchTaskResult(List<LogGroupData> fetchData, String cursor, int rawSize) {
        super(null);
        this.fetchData = fetchData;
        this.cursor = cursor;
        this.rawSize = rawSize;
    }

    public List<LogGroupData> getFetchedData() {
        return fetchData;
    }

    public String getCursor() {
        return cursor;
    }

    public int getRawSize() {
        return rawSize;
    }
}

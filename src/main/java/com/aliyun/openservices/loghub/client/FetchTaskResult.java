package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;

public class FetchTaskResult extends TaskResult {

	private List<LogGroupData> mFetchData;
	private String mCursor;

	public FetchTaskResult(List<LogGroupData> fetchData, String cursor) {
		super(null);
		mFetchData = fetchData;
		mCursor = cursor;
	}

	public List<LogGroupData> getFetchedData() {
		return mFetchData;
	}

	public String getCursor() {
		return mCursor;
	}

	public FetchTaskResult(Exception e) {
		super(e);
	}

}

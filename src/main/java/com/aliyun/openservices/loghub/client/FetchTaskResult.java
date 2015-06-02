package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.loghub.common.LogGroup;

public class FetchTaskResult extends TaskResult {

	private List<LogGroup> mFetchData;
	private String mCursor;

	public FetchTaskResult(List<LogGroup> fetchData, String cursor) {
		super(null);
		mFetchData = fetchData;
		mCursor = cursor;
	}

	public List<LogGroup> getFetchedData() {
		return mFetchData;
	}

	public String getCursor() {
		return mCursor;
	}

	public FetchTaskResult(Exception e) {
		super(e);
	}

}

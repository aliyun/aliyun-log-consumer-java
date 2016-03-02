package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;

public class FetchTaskResult extends TaskResult {

	private List<LogGroupData> mFetchData;
	private String mCursor;
	private int mRawSize;

	public FetchTaskResult(List<LogGroupData> fetchData, String cursor, int rawSize) {
		super(null);
		mFetchData = fetchData;
		mCursor = cursor;
		mRawSize = rawSize;
	}

	public List<LogGroupData> getFetchedData() {
		return mFetchData;
	}

	public String getCursor() {
		return mCursor;
	}
	public int getRawSize()
	{
		return mRawSize;
	}
	public FetchTaskResult(Exception e) {
		super(e);
	}
	
}

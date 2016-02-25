package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;

public class FetchTaskResult extends TaskResult {

	private List<LogGroupData> mFetchData;
	private String mCursor;
	private int mRawSize;
	private int mLogsCount;

	public FetchTaskResult(List<LogGroupData> fetchData, String cursor, int rawSize, int logsCount) {
		super(null);
		mFetchData = fetchData;
		mCursor = cursor;
		mRawSize = rawSize;
		mLogsCount = logsCount;
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
	public int getLogsCount()
	{
		return mLogsCount;
	}
	public FetchTaskResult(Exception e) {
		super(e);
	}
	
}

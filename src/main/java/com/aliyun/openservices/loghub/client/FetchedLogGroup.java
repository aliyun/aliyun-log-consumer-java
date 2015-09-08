package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.sls.common.LogGroupData;

public class FetchedLogGroup {
	public final String mShardId;
	public final List<LogGroupData> mFetchedData;
	public final String mEndCursor;

	public FetchedLogGroup(String shardId, List<LogGroupData> fetchedData, String endCursor) {
		mShardId = shardId;
		mFetchedData = fetchedData;
		mEndCursor = endCursor;
	}
}

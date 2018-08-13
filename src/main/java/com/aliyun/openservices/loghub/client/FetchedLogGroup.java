package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;

public class FetchedLogGroup {
	public final int mShardId;
	public final List<LogGroupData> mFetchedData;
	public final String mEndCursor;

	public FetchedLogGroup(int shardId, List<LogGroupData> fetchedData, String endCursor) {
		mShardId = shardId;
		mFetchedData = fetchedData;
		mEndCursor = endCursor;
	}
}

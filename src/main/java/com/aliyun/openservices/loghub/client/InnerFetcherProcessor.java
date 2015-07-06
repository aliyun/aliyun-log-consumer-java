package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.FetchedLogGroup;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubShardListener;
import com.aliyun.openservices.sls.common.LogGroupData;

public class InnerFetcherProcessor implements ILogHubProcessor {
	
	private String mShardId;
	private final ClientFetcher mFetcher;
	
	public InnerFetcherProcessor(ClientFetcher fetcher) {
		mFetcher = fetcher;
	}

	@Override
	public void initialize(String shardId) {
		mShardId = shardId;
		
		ILogHubShardListener listener = mFetcher.getShardListener();
		if (listener != null)
			listener.ShardAdded(mShardId);
	}

	@Override
	public void process(List<LogGroupData> logGroups,
			ILogHubCheckPointTracker checkPointTracker) {
		
		if (logGroups.size() > 0)
		{
			//get latest cursor for current data saved inside default checkpoint tracker.
			DefaultLogHubCHeckPointTracker tracker = 
				(DefaultLogHubCHeckPointTracker)checkPointTracker;
		
			FetchedLogGroup data = new FetchedLogGroup(mShardId, logGroups, tracker.getCursor());
		
			mFetcher.updateCachedData(mShardId, data);
		}
	}

	@Override
	public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
		
		//remove all the cached data (before here, no more data will be fetched)
		mFetcher.cleanCachedData(mShardId);
		
		ILogHubShardListener listener = mFetcher.getShardListener();
		if (listener != null)
			listener.ShardDeleted(mShardId);		
	}	
}

package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.FetchedLogGroup;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubShardListener;
import com.aliyun.openservices.log.common.LogGroupData;

public class InnerFetcherProcessor implements ILogHubProcessor {
	
	private int mShardId;
	private final ClientFetcher mFetcher;
	
	//because shutdown is asynchronous operation, shutdown callback may be called multiple times. this flag
	//is dedicated to avoid duplicated operation/notification because of multi-shutdown calling.
	private boolean bHasShutdown = false;
	
	public InnerFetcherProcessor(ClientFetcher fetcher) {
		mFetcher = fetcher;
	}

	@Override
	public void initialize(int shardId) {
		mShardId = shardId;
		
		ILogHubShardListener listener = mFetcher.getShardListener();
		if (listener != null)
			listener.ShardAdded(mShardId);
	}

	@Override
	public String process(List<LogGroupData> logGroups,
			ILogHubCheckPointTracker checkPointTracker) {
		
		if (logGroups.size() > 0)
		{
			//get latest cursor for current data saved inside default checkpoint tracker.
			DefaultLogHubCheckPointTracker tracker = 
				(DefaultLogHubCheckPointTracker)checkPointTracker;
		
			FetchedLogGroup data = new FetchedLogGroup(mShardId, logGroups, tracker.getCursor());
		
			mFetcher.updateCachedData(mShardId, data);
		}
		return "";
	}

	@Override
	public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
		
		if (!bHasShutdown) {
			//remove all the cached data (before here, no more data will be fetched)
			mFetcher.cleanCachedData(mShardId);
		
			ILogHubShardListener listener = mFetcher.getShardListener();
			if (listener != null)
				listener.ShardDeleted(mShardId);	
			
			bHasShutdown = true;
		}
	}	
}

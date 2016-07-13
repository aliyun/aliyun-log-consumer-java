package com.aliyun.openservices.loghub.client;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.FetchedLogGroup;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubShardListener;

public class ClientFetcher {
	
	private final ILogHubProcessorFactory mLogHubProcessorFactory;
	private final LogHubConfig mLogHubConfig;
	private final LogHubHeartBeat mLogHubHeartBeat;
	private LogHubClientAdapter mLogHubClientAdapter;
	private final Map<Integer, LogHubConsumer> mShardConsumer = new HashMap<Integer, LogHubConsumer>();
	int _curShardIndex = 0;
	private final List<Integer> mShardList = new ArrayList<Integer>();
	private final Map<Integer, FetchedLogGroup> mCachedData 
		= new HashMap<Integer, FetchedLogGroup>();
	
	private final ExecutorService mExecutorService = Executors.newCachedThreadPool();
	private final ScheduledExecutorService mShardListUpdateService = Executors.newScheduledThreadPool(1);
	
	private final long mShardListUpdateIntervalInMills = 500L;
	
	private ILogHubShardListener mLogHubShardListener = null;
	
	private static final Logger logger = Logger.getLogger(ClientFetcher.class);

	public ClientFetcher(LogHubConfig config) throws LogHubClientWorkerException {
		mLogHubProcessorFactory = new InnerFetcherProcessorFactory(this);
		mLogHubConfig = config;
		
		mLogHubClientAdapter = new LogHubClientAdapter(
				config.getLogHubEndPoint(), config.getAccessId(), config.getAccessKey(), config.getStsToken(), config.getProject(),
				config.getLogStore(), config.getConsumerGroupName(), config.getWorkerInstanceName());
		try 
		{
			mLogHubClientAdapter.CreateConsumerGroup((int)(config.getHeartBeatIntervalMillis()*2/1000), config.isConsumeInOrder());
		} 
		catch (LogException e) 
		{
			if(e.GetErrorCode().compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0)
			{
				try 
				{
					ConsumerGroup consumerGroup = mLogHubClientAdapter.GetConsumerGroup();
					if(consumerGroup != null)
					{
						if(consumerGroup.isInOrder() != mLogHubConfig.isConsumeInOrder() || consumerGroup.getTimeout() != (int)(mLogHubConfig.getHeartBeatIntervalMillis()*2/1000))
						{
							throw new LogHubClientWorkerException("consumer group is not agreed, AlreadyExistedConsumerGroup: {\"consumeInOrder\": " + consumerGroup.isInOrder() + ", \"timeoutInSecond\": " + consumerGroup.getTimeout() + "}");
						}
					}
					else
					{
						throw new LogHubClientWorkerException("consumer group not exist");
					}
				} 
				catch (LogException e1) 
				{
					throw new LogHubClientWorkerException("error occour when get consumer group, errorCode: " + e1.GetErrorCode() + ", errorMessage: " + e1.GetErrorMessage());
				}
			}
			else
			{
				throw new LogHubClientWorkerException("error occour when create consumer group, errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage());
			}
		}
		mLogHubHeartBeat = new LogHubHeartBeat(mLogHubClientAdapter, config.getHeartBeatIntervalMillis());
	}
	public void SwitchClient(String accessKeyId, String accessKey)
	{
		mLogHubClientAdapter.SwitchClient(mLogHubConfig.getLogHubEndPoint(), accessKeyId, accessKey, null);
	}
	public void SwitchClient(String accessKeyId, String accessKey, String stsToken)
	{
		mLogHubClientAdapter.SwitchClient(mLogHubConfig.getLogHubEndPoint(), accessKeyId, accessKey, stsToken);
	}
	public void start() {
		mLogHubHeartBeat.Start();
		mShardListUpdateService.scheduleWithFixedDelay(new ShardListUpdator(), 0L,
				mShardListUpdateIntervalInMills, TimeUnit.MILLISECONDS);	
		
	}
	
	/**
	 * Do not fetch any more data after calling shutdown (otherwise, the checkpoint may
	 * not be saved back in time). And lease coordinator's stop give enough time to save 
	 * checkpoint at the same time.
	 */
	public void shutdown() {

		mLogHubHeartBeat.Stop();
		mShardListUpdateService.shutdown();
	}
	
	public void registerShardListener(ILogHubShardListener listener) {
		mLogHubShardListener = listener;
	}
	
	public ILogHubShardListener getShardListener() {
		return mLogHubShardListener;
	}
	
	public FetchedLogGroup nextNoBlock()
	{
		FetchedLogGroup result = null;
		
		synchronized(mShardList) {
		      	
		    for (int i = 0; i < mShardList.size(); i++) {	
		    	// in case the number of fetcher decreased
		    	_curShardIndex = _curShardIndex % mShardList.size();
		    	int shardId = mShardList.get(_curShardIndex);
				result = mCachedData.get(shardId);
				mCachedData.put(shardId, null);
				
				//emit next consume on current shard no matter whether gotten data.
				LogHubConsumer consumer = mShardConsumer.get(shardId);
				if (consumer != null)
					consumer.consume();
				
				_curShardIndex = (_curShardIndex + 1) % mShardList.size();
		        if (result != null) {
		        	break;
		        }
		   }
		}
       
        return result;
	}	
	
	public void saveCheckPoint(int shardId, String cursor, boolean persistent) 
			throws LogHubCheckPointException {
		
		synchronized(mShardList) {
			LogHubConsumer consumer = mShardConsumer.get(shardId);
			if (consumer != null)
			{
				consumer.saveCheckPoint(cursor, persistent);
			}
			else
			{
				throw new LogHubCheckPointException("Invalid shardId when saving checkpoint");
			}
		}
	}
	
	/*
	 * update cached data from internal processor (not used externally by end users)
	 */
	public void updateCachedData(int shardId, FetchedLogGroup data) {
		synchronized(mShardList) {
			mCachedData.put(shardId, data);
		}
	}

	/*
	 * clean cached data from internal processor (not used externally by end users)
	 */	
	public void cleanCachedData(int shardId) {
		synchronized(mShardList) {
			mCachedData.remove(shardId);
		}
	}
	
	private class ShardListUpdator implements Runnable {	
		public void run() {
			try {
				ArrayList<Integer> heldShards = new ArrayList<Integer>();
				mLogHubHeartBeat.GetHeldShards(heldShards);
				for(int shard: heldShards)
				{
					getConsuemr(shard);
				}
				cleanConsumer(heldShards);
			} catch (Throwable t) {

			}		
		}
	}	
	
	private void cleanConsumer(ArrayList<Integer> ownedShard)
	{
		synchronized(mShardList) {
			ArrayList<Integer> removeShards = new ArrayList<Integer>();
			for (Entry<Integer, LogHubConsumer> shard : mShardConsumer.entrySet())
			{
				LogHubConsumer consumer = shard.getValue();
				if (!ownedShard.contains(shard.getKey()))
				{
					consumer.shutdown();
				}
				if (consumer.isShutdown())
				{
					mLogHubHeartBeat.RemoveHeartShard(shard.getKey());
					mShardConsumer.remove(shard.getKey());
					removeShards.add(shard.getKey());
					mShardList.remove(shard.getKey());
				}
			}
			for(int shard: removeShards)
			{
				mShardConsumer.remove(shard);
			}
		}
	}
	
	private LogHubConsumer getConsuemr(int shardId)
	{
		synchronized(mShardList) {
			LogHubConsumer consumer = mShardConsumer.get(shardId);
			if (consumer != null)
			{
				return consumer;
			}
			consumer = new LogHubConsumer(mLogHubClientAdapter,shardId,
					mLogHubConfig.getWorkerInstanceName(),
					mLogHubProcessorFactory.generatorProcessor(), mExecutorService,
					mLogHubConfig.getCursorPosition(),
					mLogHubConfig.GetCursorStartTime());
			mShardConsumer.put(shardId, consumer);
			mShardList.add(shardId);
			consumer.consume();
			return consumer;
		}
	}
}

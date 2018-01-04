package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;


public class ClientWorker implements Runnable
{
	
	private final ILogHubProcessorFactory mLogHubProcessorFactory;
	private final LogHubConfig mLogHubConfig;
	private final LogHubHeartBeat mLogHubHeartBeat;
	private boolean mShutDown = false;
	private final Map<Integer, LogHubConsumer> mShardConsumer = new HashMap<Integer, LogHubConsumer>();
	private final ExecutorService mExecutorService = Executors.newCachedThreadPool(new LogThreadFactory());
	private LogHubClientAdapter mLogHubClientAdapter;
	private static final Logger logger = Logger.getLogger(ClientWorker.class);
	private boolean mMainLoopExit = false;

	public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
		mLogHubProcessorFactory = factory;
		mLogHubConfig = config;
		mLogHubClientAdapter = new LogHubClientAdapter(
				config.getLogHubEndPoint(), config.getAccessId(), config.getAccessKey(), config.getStsToken(), config.getProject(),
				config.getLogStore(), config.getConsumerGroupName(), config.getWorkerInstanceName(), config.isDirectModeEnabled());
		try 
		{
			mLogHubClientAdapter.CreateConsumerGroup((int)(config.getHeartBeatIntervalMillis()*2/1000), config.isConsumeInOrder());
		} 
		catch (LogException e) 
		{
			if(e.GetErrorCode().compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0)
			{
				try {
					mLogHubClientAdapter.UpdateConsumerGroup((int)(config.getHeartBeatIntervalMillis()*2/1000), config.isConsumeInOrder());
				} catch (LogException e1) {
					throw new LogHubClientWorkerException("error occour when update consumer group, errorCode: " + e1.GetErrorCode() + ", errorMessage: " + e1.GetErrorMessage());
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
	public void run() {		
		mLogHubHeartBeat.Start();
		ArrayList<Integer> heldShards = new ArrayList<Integer>();
		while (mShutDown == false) {
			mLogHubHeartBeat.GetHeldShards(heldShards);
			for(int shard: heldShards)
			{
				LogHubConsumer consumer = getConsuemr(shard);
				consumer.consume();
			}
			cleanConsumer(heldShards);
			try {
				Thread.sleep(mLogHubConfig.getDataFetchIntervalMillis());
			} catch (InterruptedException e) {
				
			}
		}
		mMainLoopExit = true;
	}
	public void shutdown()
	{
		this.mShutDown = true;
		int times = 0 ;
		while(!mMainLoopExit && times++ < 20) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		for(LogHubConsumer consumer: mShardConsumer.values()){
			consumer.shutdown();
		}
		mExecutorService.shutdown();
		try {
			mExecutorService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		mLogHubHeartBeat.Stop();
	}
	
	private void cleanConsumer(ArrayList<Integer> ownedShard)
	{
		ArrayList<Integer> removeShards = new ArrayList<Integer>();
		for (Entry<Integer, LogHubConsumer> shard : mShardConsumer.entrySet())
		{
			LogHubConsumer consumer = shard.getValue();
			if (!ownedShard.contains(shard.getKey()))
			{
				consumer.shutdown();
				logger.info("try to shut down a consumer shard:" + shard.getKey());
			}
			if (consumer.isShutdown())
			{
				mLogHubHeartBeat.RemoveHeartShard(shard.getKey());
				removeShards.add(shard.getKey());
				logger.info("remove a consumer shard:" + shard.getKey());
			}
		}
		for(int shard: removeShards)
		{
			mShardConsumer.remove(shard);
		}
	}
	
	private LogHubConsumer getConsuemr(final int shardId)
	{
		LogHubConsumer consumer = mShardConsumer.get(shardId);
		if (consumer != null)
		{
			return consumer;
		}
		consumer = new LogHubConsumer(mLogHubClientAdapter,shardId,
				mLogHubConfig.getConsumerName(),
				mLogHubProcessorFactory.generatorProcessor(), mExecutorService,
				mLogHubConfig.getCursorPosition(),
				mLogHubConfig.GetCursorStartTime());
		mShardConsumer.put(shardId, consumer);
		logger.info("create a consumer shard:" + shardId);
		return consumer;
	}
}

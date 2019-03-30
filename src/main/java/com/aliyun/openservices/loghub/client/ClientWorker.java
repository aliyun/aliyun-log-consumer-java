package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class ClientWorker implements Runnable {
	private final ILogHubProcessorFactory processorFactory;
	private final LogHubConfig logHubConfig;
	private final LogHubHeartBeat logHubHeartBeat;
	private boolean shutDown = false;
	private final Map<Integer, LogHubConsumer> shardConsumer = new HashMap<Integer, LogHubConsumer>();
	private final ExecutorService executorService = Executors.newCachedThreadPool(new LogThreadFactory());
	private LogHubClientAdapter logHubClientAdapter;
	private static final Logger logger = Logger.getLogger(ClientWorker.class);
	private boolean mainLoopExit = false;

	public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
		processorFactory = factory;
		logHubConfig = config;
		logHubClientAdapter = new LogHubClientAdapter(
				config.getLogHubEndPoint(), config.getAccessId(), config.getAccessKey(), config.getStsToken(), config.getProject(),
				config.getLogStore(), config.getConsumerGroupName(), config.getConsumerName(), config.isDirectModeEnabled());
		try 
		{
			logHubClientAdapter.CreateConsumerGroup((int)(config.getHeartBeatIntervalMillis()*2/1000), config.isConsumeInOrder());
		} 
		catch (LogException e) 
		{
			if(e.GetErrorCode().compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0)
			{
				try {
					logHubClientAdapter.UpdateConsumerGroup((int)(config.getHeartBeatIntervalMillis()*2/1000), config.isConsumeInOrder());
				} catch (LogException e1) {
					throw new LogHubClientWorkerException("error occour when update consumer group, errorCode: " + e1.GetErrorCode() + ", errorMessage: " + e1.GetErrorMessage());
				}
			}
			else
			{
				throw new LogHubClientWorkerException("error occour when create consumer group, errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage());
			}
		}
		logHubHeartBeat = new LogHubHeartBeat(logHubClientAdapter, config.getHeartBeatIntervalMillis());
	}
	public void SwitchClient(String accessKeyId, String accessKey)
	{
		logHubClientAdapter.SwitchClient(logHubConfig.getLogHubEndPoint(), accessKeyId, accessKey, null);
	}
	public void SwitchClient(String accessKeyId, String accessKey, String stsToken)
	{
		logHubClientAdapter.SwitchClient(logHubConfig.getLogHubEndPoint(), accessKeyId, accessKey, stsToken);
	}
	public void run() {		
		logHubHeartBeat.Start();
		ArrayList<Integer> heldShards = new ArrayList<Integer>();
		while (!shutDown) {
			logHubHeartBeat.GetHeldShards(heldShards);
			for(int shard: heldShards)
			{
				LogHubConsumer consumer = getConsumer(shard);
				consumer.consume();
			}
			cleanConsumer(heldShards);
			try {
				Thread.sleep(logHubConfig.getDataFetchIntervalMillis());
			} catch (InterruptedException e) {
				
			}
		}
		mainLoopExit = true;
	}
	public void shutdown()
	{
		this.shutDown = true;
		int times = 0 ;
		while(!mainLoopExit && times++ < 20) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		for(LogHubConsumer consumer: shardConsumer.values()){
			consumer.shutdown();
		}
		executorService.shutdown();
		try {
			executorService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		logHubHeartBeat.Stop();
	}
	
	private void cleanConsumer(ArrayList<Integer> ownedShard)
	{
		ArrayList<Integer> removeShards = new ArrayList<Integer>();
		for (Entry<Integer, LogHubConsumer> shard : shardConsumer.entrySet())
		{
			LogHubConsumer consumer = shard.getValue();
			if (!ownedShard.contains(shard.getKey()))
			{
				consumer.shutdown();
				logger.info("try to shut down a consumer shard:" + shard.getKey());
			}
			if (consumer.isShutdown())
			{
				logHubHeartBeat.RemoveHeartShard(shard.getKey());
				removeShards.add(shard.getKey());
				logger.info("remove a consumer shard:" + shard.getKey());
			}
		}
		for(int shard: removeShards)
		{
			shardConsumer.remove(shard);
		}
	}
	
	private LogHubConsumer getConsumer(final int shardId)
	{
		LogHubConsumer consumer = shardConsumer.get(shardId);
		if (consumer != null)
		{
			return consumer;
		}
		consumer = new LogHubConsumer(logHubClientAdapter,shardId,
				logHubConfig.getConsumerName(),
				processorFactory.generatorProcessor(), executorService,
				logHubConfig.getCursorPosition(),
				logHubConfig.GetCursorStartTime(),
				logHubConfig.getMaxFetchLogGroupSize());
		shardConsumer.put(shardId, consumer);
		logger.info("create a consumer shard:" + shardId);
		return consumer;
	}
}

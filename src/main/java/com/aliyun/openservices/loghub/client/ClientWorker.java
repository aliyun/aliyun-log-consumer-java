package com.aliyun.openservices.loghub.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;
import com.aliyun.openservices.loghub.client.lease.LogHubLeaseCoordinator;


public class ClientWorker implements Runnable {
	
	private final ILogHubProcessorFactory mLogHubProcessorFactory;
	private final LogHubConfig mLogHubConfig;
	private final ILogHubLeaseManager mLeaseManager;
	private final LogHubLeaseCoordinator mLeaseCorordinator;
	private boolean mShutDown = false;
	private final Map<String, LogHubConsumer> mShardConsumer = new HashMap<String, LogHubConsumer>();
	private final ExecutorService mExecutorService = Executors.newCachedThreadPool();
	
	private static final Logger logger = Logger.getLogger(ClientWorker.class);

	public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config, ILogHubLeaseManager leaseManager) {
		mLogHubProcessorFactory = factory;
		mLogHubConfig = config;
		mLeaseManager = leaseManager;
		
		SLSClient loghubClient = new SLSClient(
				config.getLogHubEndPoint(), config.getAccessId(),
				config.getAccessKey());
		LogHubClientAdapter clientAdpater = new LogHubClientAdapter(
				loghubClient, config.getProject(),
				config.getLogStore());
		mLeaseCorordinator = new LogHubLeaseCoordinator(clientAdpater,
				mLeaseManager, config.getWorkerInstanceName(),
				config.getLeaseDurtionTimeMillis());
	}
	
	
	public void run() {
		try {
			mLeaseManager.Initilize(mLogHubConfig.getConsumerGroupName(), mLogHubConfig.getWorkerInstanceName(),
					mLogHubConfig.getProject(), mLogHubConfig.getLogStore());
		} catch (LogHubLeaseException e) {
			logger.error("Failed to init the loghub worker client env, exit", e);
			return;
		}
		
		mLeaseCorordinator.start();
		while (mShutDown == false) {
			Map<String, LogHubLease> allLogHubLease = mLeaseCorordinator
					.getAllHeldLease();
			for (Map.Entry<String, LogHubLease> entry : allLogHubLease
					.entrySet()) {
				String shardId = entry.getKey();
				LogHubLease lease = entry.getValue();
				if (lease.isConsumerHoldLease()) {
					LogHubConsumer consumer = getConsuemr(shardId);
					consumer.consume();
				}
			}
			cleanConsumer(allLogHubLease.keySet());
			try {
				Thread.sleep(mLogHubConfig.getDataFetchIntervalMillis());
			} catch (InterruptedException e) {

			}
		}
	}
	public void shutdown()
	{
		this.mShutDown = true;
	}
	
	private void cleanConsumer(Set<String> ownedShard)
	{
		Set<String> processingShard = new HashSet<String>(mShardConsumer.keySet());
		for (String shardId : processingShard)
		{
			LogHubConsumer consumer = mShardConsumer.get(shardId);
			if (ownedShard.contains(shardId) == false)
			{
				consumer.shutdown();
				logger.warn("try to shut down a consumer shard:" + shardId);
			}
			if (consumer.isShutdown())
			{
				mShardConsumer.remove(shardId);
				logger.warn("remove a consumer shard:" + shardId);
			}
		}
	}
	
	private LogHubConsumer getConsuemr(String shardId)
	{
		LogHubConsumer consumer = mShardConsumer.get(shardId);
		if (consumer != null)
		{
			return consumer;
		}
		
		SLSClient loghubClient = new SLSClient(
				mLogHubConfig.getLogHubEndPoint(), mLogHubConfig.getAccessId(),
				mLogHubConfig.getAccessKey());
		consumer = new LogHubConsumer(loghubClient,
				mLogHubConfig.getProject(),
				mLogHubConfig.getLogStore(), shardId,
				mLogHubConfig.getWorkerInstanceName(), mLeaseManager,
				mLogHubProcessorFactory.generatorProcessor(), mExecutorService, mLogHubConfig.getCursorPosition());
		mShardConsumer.put(shardId, consumer);
		logger.warn("create a consumer shard:" + shardId);
		return consumer;
	}
}

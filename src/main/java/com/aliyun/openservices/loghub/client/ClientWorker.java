package com.aliyun.openservices.loghub.client;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.LogHubClient;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;
import com.aliyun.openservices.loghub.client.lease.LogHubLeaseCoordinator;
import com.aliyun.openservices.loghub.client.lease.impl.MySqlLogHubLeaseManager;

public class ClientWorker implements Runnable {
	
	private final ILogHubProcessorFactory mLogHubProcessorFactory;
	private final LogHubConfig mLogHubConfig;
	private final MySqlLogHubLeaseManager mLeaseManager;
	private final LogHubLeaseCoordinator mLeaseCorordinator;
	private boolean mShutDown = false;
	private final Map<String, LogHubConsumer> mShardConsumer = new HashMap<String, LogHubConsumer>();
	private final ExecutorService mExecutorService = Executors.newCachedThreadPool();
	protected Logger logger = Logger.getLogger(this.getClass());

	public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) {
		mLogHubProcessorFactory = factory;
		mLogHubConfig = config;
		String sigBody = config.getLogHubProject() + "#" + config.getLogHubStreamName();
		String md5Value = GetMd5Value(sigBody.toLowerCase());
		mLeaseManager = new MySqlLogHubLeaseManager(
				config.getConsumeGroupName(), md5Value, config.getDbConfig());
		LogHubClient loghubClient = new LogHubClient(
				config.getLogHubEndPoint(), config.getAccessId(),
				config.getAccessKey());
		LogHubClientAdapter clientAdpater = new LogHubClientAdapter(
				loghubClient, config.getLogHubProject(),
				config.getLogHubStreamName());
		mLeaseCorordinator = new LogHubLeaseCoordinator(clientAdpater,
				mLeaseManager, config.getWorkerInstanceName(),
				config.getLeaseDurtionTimeMillis());
	}
	
	private String GetMd5Value(String body) {
		try {
			byte[] bytes = body.getBytes("utf-8");
			MessageDigest md;
			md = MessageDigest.getInstance("MD5");
			String res = new BigInteger(1, md.digest(bytes)).toString(16)
					.toUpperCase();

			StringBuilder zeros = new StringBuilder();
			for (int i = 0; i + res.length() < 32; i++) {
				zeros.append("0");
			}
			return zeros.toString() + res;
		} catch (NoSuchAlgorithmException e) {
			return "";
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	
	public void run() {
	
		try {
			mLeaseManager.Initilize();
			mLeaseManager.registerWorker(this.mLogHubConfig
					.getWorkerInstanceName());
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
		Set<String> processingShard = mShardConsumer.keySet();
		for (String shardId : processingShard)
		{
			LogHubConsumer consumer = mShardConsumer.get(shardId);
			if (ownedShard.contains(shardId) == false)
			{
				consumer.shutdown();
			}
			if (consumer.isShutdown())
			{
				mShardConsumer.remove(shardId);
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
		
		LogHubClient loghubClient = new LogHubClient(
				mLogHubConfig.getLogHubEndPoint(), mLogHubConfig.getAccessId(),
				mLogHubConfig.getAccessKey());
		consumer = new LogHubConsumer(loghubClient,
				mLogHubConfig.getLogHubProject(),
				mLogHubConfig.getLogHubStreamName(), shardId,
				mLogHubConfig.getWorkerInstanceName(), mLeaseManager,
				mLogHubProcessorFactory.generatorProcessor(), mExecutorService, mLogHubConfig.getCursorPosition());
		mShardConsumer.put(shardId, consumer);
		return consumer;
	}
}

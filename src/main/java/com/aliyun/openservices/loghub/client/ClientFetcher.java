package com.aliyun.openservices.loghub.client;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.loghub.client.FetchedLogGroup;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubShardListener;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;
import com.aliyun.openservices.loghub.client.lease.LogHubLeaseCoordinator;
import com.aliyun.openservices.loghub.client.lease.impl.MySqlLogHubLeaseManager;

public class ClientFetcher {
	
	private final ILogHubProcessorFactory mLogHubProcessorFactory;
	private final LogHubConfig mLogHubConfig;
	private final MySqlLogHubLeaseManager mLeaseManager;
	private final LogHubLeaseCoordinator mLeaseCorordinator;
	
	private final Map<String, LogHubConsumer> mShardConsumer = 
			new HashMap<String, LogHubConsumer>();

	int _curShardIndex = 0;
	private final List<String> mShardList = new ArrayList<String>();
	private final Map<String, FetchedLogGroup> mCachedData 
		= new HashMap<String, FetchedLogGroup>();
	
	private final ExecutorService mExecutorService = Executors.newCachedThreadPool();
	private final ScheduledExecutorService mShardListUpdateService = Executors.newScheduledThreadPool(1);
	
	private final long mShardListUpdateIntervalInMills = 500L;
	
	private ILogHubShardListener mLogHubShardListener = null;
	
	private static final Logger logger = Logger.getLogger(ClientFetcher.class);

	public ClientFetcher(LogHubConfig config) {
		mLogHubProcessorFactory = new InnerFetcherProcessorFactory(this);
		mLogHubConfig = config;
		String sigBody = config.getLogHubProject() + "#" + config.getLogHubStreamName();
		String md5Value = GetMd5Value(sigBody.toLowerCase());
		mLeaseManager = new MySqlLogHubLeaseManager(
				config.getConsumeGroupName(), md5Value, config.getDbConfig());
		SLSClient loghubClient = new SLSClient(
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
	
	
	public void start() {

		try {
			mLeaseManager.Initilize();
			mLeaseManager.registerWorker(this.mLogHubConfig
					.getWorkerInstanceName());
		} catch (LogHubLeaseException e) {
			logger.error("Failed to init the loghub worker client env, exit", e);
			return;
		}
		
		mLeaseCorordinator.start();	
		
		mShardListUpdateService.scheduleWithFixedDelay(new ShardListUpdator(), 0L,
				mShardListUpdateIntervalInMills, TimeUnit.MILLISECONDS);	
		
	}
	
	/**
	 * Do not fetch any more data after calling shutdown (otherwise, the checkpoint may
	 * not be saved back in time). And lease coordinator's stop give enough time to save 
	 * checkpoint at the same time.
	 */
	public void shutdown() {

		mLeaseCorordinator.stop();
		mShardListUpdateService.shutdown();
	}
	
	public void registerShardListener(ILogHubShardListener listener) {
		mLogHubShardListener = listener;
	}
	
	public ILogHubShardListener getShardListener() {
		return mLogHubShardListener;
	}
	
	/*
	public LogHubFetchData nextBlockIfNotFound()
	{
		
	}
	*/
	
	public FetchedLogGroup nextNoBlock()
	{
		FetchedLogGroup result = null;
		
		synchronized(mShardList) {
		      	
		    for (int i = 0; i < mShardList.size(); i++) {	
		    	// in case the number of fetcher decreased
		    	_curShardIndex = _curShardIndex % mShardList.size();
		    	String shardId = mShardList.get(_curShardIndex);
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
	
	public void saveCheckPoint(String shardId, String cursor, boolean persistent) 
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
	public void updateCachedData(String shardId, FetchedLogGroup data) {
		synchronized(mShardList) {
			mCachedData.put(shardId, data);
		}
	}

	/*
	 * clean cached data from internal processor (not used externally by end users)
	 */	
	public void cleanCachedData(String shardId) {
		synchronized(mShardList) {
			mCachedData.remove(shardId);
		}
	}
	
	private class ShardListUpdator implements Runnable {	
		public void run() {
			try {
					Map<String, LogHubLease> allLogHubLease = mLeaseCorordinator
							.getAllHeldLease();
					for (Map.Entry<String, LogHubLease> entry : allLogHubLease
							.entrySet()) {
						String shardId = entry.getKey();
						LogHubLease lease = entry.getValue();
						if (lease.isConsumerHoldLease()) {
							getConsuemr(shardId); 
						}
					}					
					cleanConsumer(allLogHubLease.keySet());
			} catch (Throwable t) {

			}		
		}
	}	
	
	private void cleanConsumer(Set<String> ownedShard)
	{
		synchronized(mShardList) {
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
					mShardList.remove(shardId);
				}
			}
		}
	}
	
	private LogHubConsumer getConsuemr(String shardId)
	{
		synchronized(mShardList) {
			LogHubConsumer consumer = mShardConsumer.get(shardId);
			if (consumer != null)
			{
				return consumer;
			}
			
			SLSClient loghubClient = new SLSClient(
					mLogHubConfig.getLogHubEndPoint(), mLogHubConfig.getAccessId(),
					mLogHubConfig.getAccessKey());
			consumer = new LogHubConsumer(loghubClient,
					mLogHubConfig.getLogHubProject(),
					mLogHubConfig.getLogHubStreamName(), shardId,
					mLogHubConfig.getWorkerInstanceName(), mLeaseManager,
					mLogHubProcessorFactory.generatorProcessor(), mExecutorService, mLogHubConfig.getCursorPosition());
			mShardConsumer.put(shardId, consumer);
			mShardList.add(shardId);
			
			logger.info("new consumer is added for shard " + shardId);
			
			//trigger first consume operation to emit Initialize task.
			consumer.consume();
			return consumer;
		}
	}
}

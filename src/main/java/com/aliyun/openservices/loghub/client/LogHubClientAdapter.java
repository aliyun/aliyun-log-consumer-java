package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.Logs.LogGroup;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;

public class LogHubClientAdapter {

	private Client mClient;
	private ReadWriteLock mReadWrtlock = new ReentrantReadWriteLock(); 
	private final String mProject;
	private final String mStream;
	private final String mConsumerGroup;
	private final String mConsumer;
	private final String mUserAgent;
	private static final Logger logger = Logger.getLogger(LogHubClientAdapter.class);
	
	public LogHubClientAdapter(String endPoint, String accessKeyId, String accessKey, String stsToken, String project, String stream,
			String consumerGroup, String consumer) 
	{
		super();
		this.mClient = new Client(endPoint, accessKeyId, accessKey);
		if(stsToken != null)
		{
			this.mClient.SetSecurityToken(stsToken);
		}
		this.mProject = project;
		this.mStream = stream;
		this.mConsumerGroup = consumerGroup;
		this.mConsumer = consumer;
		this.mUserAgent = "[consumer-group-java]" + consumerGroup;
		this.mClient.setUserAgent(mUserAgent);
	}
	public void SwitchClient(String endPoint, String accessKeyId, String accessKey, String stsToken)
	{
		mReadWrtlock.writeLock().lock();
		this.mClient = new Client(endPoint, accessKeyId, accessKey);
		if(stsToken != null)
		{
			this.mClient.SetSecurityToken(stsToken);
		}
		mReadWrtlock.writeLock().unlock();
	}
	public void CreateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException
	{
		mReadWrtlock.readLock().lock();
		try {
			mClient.CreateConsumerGroup(mProject, mStream, new ConsumerGroup(mConsumerGroup, timeoutInSec, inOrder));
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
	}
	
	public ConsumerGroup GetConsumerGroup() throws LogException
	{
		mReadWrtlock.readLock().lock();
		try {
			for(ConsumerGroup consumerGroup: mClient.ListConsumerGroup(mProject, mStream).GetConsumerGroups())
			{
				if(consumerGroup.getConsumerGroupName().compareTo(mConsumerGroup) == 0)
				{
					return consumerGroup;
				}
			}
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
		return null;
	}
	
	public boolean HeartBeat(ArrayList<Integer> shards, ArrayList<Integer> response)
	{
		mReadWrtlock.readLock().lock();
		response.clear();
		try {
			response.addAll(mClient.HeartBeat(mProject, mStream, mConsumerGroup, mConsumer, shards).GetShards());
			//System.out.println(response.toString());
			return true;
		} catch (LogException e) {
			logger.warn(e.GetErrorCode() + ": " + e.GetErrorMessage());
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
		return false;
	}
	public void UpdateCheckPoint(final int shard, final String consumer, final String checkpoint) throws LogException
	{
		mReadWrtlock.readLock().lock();
		try {
			mClient.UpdateCheckPoint(mProject, mStream, mConsumerGroup, consumer, shard, checkpoint);
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
	}
	public String GetCheckPoint(final int shard) throws LogException, LogHubCheckPointException
	{
		mReadWrtlock.readLock().lock();
		ArrayList<ConsumerGroupShardCheckPoint> checkPoints = null;
		try {
			checkPoints = mClient.GetCheckPoint(mProject, mStream, mConsumerGroup, shard).GetCheckPoints();
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
		if(checkPoints == null || checkPoints.size() == 0)
		{
			throw new LogHubCheckPointException("fail to get shard checkpoint");
		}
		else
		{
			return checkPoints.get(0).getCheckPoint();
		}
	}
	public String GetCursor(final int shard, CursorMode mode) throws LogException
	{
		mReadWrtlock.readLock().lock();
		try {
			return mClient.GetCursor(mProject, mStream, shard, mode).GetCursor();
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
	}
	public String GetCursor(final int shard, final long time) throws LogException
	{
		mReadWrtlock.readLock().lock();
		try {
			return mClient.GetCursor(mProject, mStream, shard, time).GetCursor();
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
	}
	public BatchGetLogResponse BatchGetLogs(final int shard, final int lines, final String cursor) throws LogException
	{
		mReadWrtlock.readLock().lock();
		try {
			BatchGetLogResponse response = mClient.BatchGetLog(mProject, mStream, shard, lines, cursor);
			return response;
		}
		finally{
			mReadWrtlock.readLock().unlock();
		}
	}
}
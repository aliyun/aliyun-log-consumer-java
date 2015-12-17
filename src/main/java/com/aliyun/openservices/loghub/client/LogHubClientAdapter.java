package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;

public class LogHubClientAdapter {

	private final Client mClient;
	private final String mProject;
	private final String mStream;
	private final String mConsumerGroup;
	private final String mConsumer;
	private static final Logger logger = Logger.getLogger(LogHubClientAdapter.class);
	
	public LogHubClientAdapter(String endPoint, String accessKeyId, String accessKey, String project, String stream,
			String consumerGroup, String consumer) 
	{
		super();
		this.mClient = new Client(endPoint, accessKeyId, accessKey);
		this.mProject = project;
		this.mStream = stream;
		this.mConsumerGroup = consumerGroup;
		this.mConsumer = consumer;
	}
	
	public void CreateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException
	{
		mClient.CreateConsumerGroup(mProject, mStream, new ConsumerGroup(mConsumerGroup, timeoutInSec, inOrder));
	}
	
	public ConsumerGroup GetConsumerGroup() throws LogException
	{
		for(ConsumerGroup consumerGroup: mClient.ListConsumerGroup(mProject, mStream).GetConsumerGroups())
		{
			if(consumerGroup.getConsumerGroupName().compareTo(mConsumerGroup) == 0)
			{
				return consumerGroup;
			}
		}
		return null;
	}
	
	public boolean HeartBeat(ArrayList<Integer> shards, ArrayList<Integer> response)
	{
		response.clear();
		try {
			response.addAll(mClient.HeartBeat(mProject, mStream, mConsumerGroup, mConsumer, shards).GetShards());
			return true;
		} catch (LogException e) {
			logger.warn(e.GetErrorCode() + ": " + e.GetErrorMessage());
		}
		return false;
	}
	public void UpdateCheckPoint(final int shard, final String consumer, final String checkpoint) throws LogException
	{
		mClient.UpdateCheckPoint(mProject, mStream, mConsumerGroup, consumer, shard, checkpoint);
	}
	public String GetCheckPoint(final int shard) throws LogException, LogHubCheckPointException
	{
		ArrayList<ConsumerGroupShardCheckPoint> checkPoints = mClient.GetCheckPoint(mProject, mStream, mConsumerGroup, shard).GetCheckPoints();
		if(checkPoints.size() > 0)
		{
			return checkPoints.get(0).getCheckPoint();
		}
		else
		{
			throw new LogHubCheckPointException("fail to get shard checkpoint");
		}
	}
	public String GetCursor(final int shard, CursorMode mode) throws LogException
	{
		return mClient.GetCursor(mProject, mStream, shard, mode).GetCursor();
	}
	public String GetCursor(final int shard, final long time) throws LogException
	{
		return mClient.GetCursor(mProject, mStream, shard, time).GetCursor();
	}
	public BatchGetLogResponse BatchGetLogs(final int shard, final int lines, final String cursor) throws LogException
	{
		return mClient.BatchGetLog(mProject, mStream, shard, lines, cursor);
	}
}

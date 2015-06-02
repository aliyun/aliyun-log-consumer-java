package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.aliyun.openservices.loghub.LogHubClient;
import com.aliyun.openservices.loghub.common.ShardResource;
import com.aliyun.openservices.loghub.exception.LogHubClientException;
import com.aliyun.openservices.loghub.exception.LogHubException;

public class LogHubClientAdapter {

	private final LogHubClient mClient;
	private final String mProject;
	private final String mStream;
	
	public LogHubClientAdapter(LogHubClient client, String project,
			String stream) {
		mClient = client;
		mProject = project;
		mStream = stream;
	}
	public List<String> listShard()
	{
		List<String> shards = new ArrayList<String>();
		try {
			Vector<ShardResource>  res = mClient.listShard(mProject, mStream).getShards();
			for(ShardResource resource : res)
			{
		//		if (resource.getShardStatus().equals("OK"))
				{
					shards.add(String.valueOf(resource.getShardId()));
				}
			}
		} catch (LogHubException e) {
			
		} catch (LogHubClientException e) {
			
		}
		return shards;
	}
}

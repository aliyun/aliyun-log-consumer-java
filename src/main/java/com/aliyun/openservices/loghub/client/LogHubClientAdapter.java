package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.sls.common.Shard;
import com.aliyun.openservices.sls.exception.SlsException;

public class LogHubClientAdapter {

	private final SLSClient mClient;
	private final String mProject;
	private final String mStream;
	
	public LogHubClientAdapter(SLSClient client, String project,
			String stream) {
		mClient = client;
		mProject = project;
		mStream = stream;
	}
	public List<String> listShard()
	{
		List<String> shards = new ArrayList<String>();
		try {
			List<Shard>  res = mClient.ListShard(mProject, mStream).GetShards();
			for(Shard resource : res)
			{
		//		if (resource.getShardStatus().equals("OK"))
				{
					shards.add(String.valueOf(resource.GetShardId()));
				}
			}
		} catch (SlsException e) {
			
		} 
		
		return shards;
	}
}

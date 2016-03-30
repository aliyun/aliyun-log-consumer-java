package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;

public class DebugHelper 
{
	public static boolean InitCheckPoint(LogHubConfig config)
	{
		Client client = new Client(config.getLogHubEndPoint(), config.getAccessId(), config.getAccessKey());
		if(config.getStsToken() != null && !config.getStsToken().isEmpty())
		{
			client.SetSecurityToken(config.getStsToken());
		}
		try {
			for(Shard s: client.ListShard(config.getProject(), config.getLogStore()).GetShards())
			{
				String cursor = "";
				if(config.getCursorPosition() == LogHubCursorPosition.BEGIN_CURSOR)
				{
					cursor = client.GetCursor(config.getProject(), config.getLogStore(), s.GetShardId(), CursorMode.BEGIN).GetCursor();
				}
				else if(config.getCursorPosition() == LogHubCursorPosition.END_CURSOR)
				{
					cursor = client.GetCursor(config.getProject(), config.getLogStore(), s.GetShardId(), CursorMode.END).GetCursor();
				}
				else
				{
					cursor = client.GetCursor(config.getProject(), config.getLogStore(), s.GetShardId(), config.GetCursorStartTime()).GetCursor();
				}
				client.UpdateCheckPoint(config.getProject(), config.getLogStore(), config.getConsumerGroupName(), s.GetShardId(), cursor);
			}
		} catch (LogException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public static void main(String[] args) 
	{
	}

}

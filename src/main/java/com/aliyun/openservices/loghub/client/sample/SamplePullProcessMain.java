package com.aliyun.openservices.loghub.client.sample;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.PropertyConfigurator;

import com.aliyun.openservices.loghub.client.ClientFetcher;
import com.aliyun.openservices.loghub.client.FetchedLogGroup;
import com.aliyun.openservices.loghub.client.config.LogHubClientDbConfig;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubShardListener;
import com.aliyun.openservices.sls.common.LogGroupData;
import com.aliyun.openservices.sls.common.LogItem;

class ShardListener implements ILogHubShardListener {
	
	public ShardListener() {
		
	}

	@Override
	public void ShardAdded(String shardId) {
		System.out.println("added new shard " + shardId);
		
	}

	@Override
	public void ShardDeleted(String shardId) {
		System.out.println("deleted new shard " + shardId);
		
	}
	
}

public class SamplePullProcessMain {

	static {
		PropertyConfigurator.configure("config" + File.separator + "log4j.properties");
	}	
	
	public static void main(String args[]) {
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				"10.101.172.22", 3306, "scmc", "apsara", "123456",
				"loghub_worker", "loghub_lease");

		System.out.println("Please input instancename for pull test:");
		Scanner sn = new Scanner(System.in);
		
		String instanceName = sn.next();

		sn.close();
		
		String project = "ali-yun-xgl";
		String stream = "logstore-xgl";

		LogHubConfig config = new LogHubConfig("consume_10_shards",
				instanceName, "cn-hangzhou-devcommon-intranet.sls.aliyuncs.com", project, stream,
				"94to3z418yupi6ikawqqd370", "DFk3ONbf81veUFpMg7FtY0BLB2w=", dbConfig,
				LogHubCursorPosition.BEGIN_CURSOR);

		ClientFetcher clientFetcher = new ClientFetcher(config);
		
		clientFetcher.registerShardListener(new ShardListener());
		
		clientFetcher.start();

		while(true) {	
			FetchedLogGroup data = clientFetcher.nextNoBlock();
			
			if (data != null) {
				List<LogGroupData> logGroups = data.mFetchedData;
				String shardId = data.mShardId;
				String cursor = data.mEndCursor;
				
				for (LogGroupData group : logGroups) {
					List<LogItem> items = group.GetAllLogs();

					for (LogItem item : items) {
						System.out.println("shard_id:" + shardId + " " + item.ToJsonString());
					}
				}
				
				try {
					//System.out.println("save checkpoint for " + shardId);
					clientFetcher.saveCheckPoint(shardId, cursor, false);
				} catch (LogHubCheckPointException e)
				{
					e.printStackTrace();
					break;
				}
			}
			else
			{
				//System.out.println("sleep for a while because there is not data...");					
				try {
					Thread.sleep(1000); //sleep for a while if no data
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}	
			
		}
		
	}

}

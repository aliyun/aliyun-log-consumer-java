package com.aliyun.openservices.loghub.client.sample;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//import com.aliyun.openservices.sls.SLSClient;
import com.aliyun.openservices.loghub.client.LogHubClientAdapter;
import com.aliyun.openservices.loghub.client.config.LogHubClientDbConfig;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;
import com.aliyun.openservices.loghub.client.lease.LogHubLeaseCoordinator;
import com.aliyun.openservices.loghub.client.lease.impl.MySqlLogHubLeaseManager;

public class loghub_client_sample {

	
	public static void main(String args[]) throws LogHubLeaseException {
		class MockLogHubClientAdapter extends LogHubClientAdapter {
			private List<String> mShards;
			public MockLogHubClientAdapter() {
				super(null, null, null);
			}

			public List<String> listShard() {
				return mShards;
			}

			public void setShard(List<String>  shards)
			{
				mShards = shards;
			}
		};
		
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				"10.101.172.22", 3306, "scmc", "apsara", "123456",
				"loghub_worker", "loghub_lease");
		MySqlLogHubLeaseManager leaseManager_1 = new MySqlLogHubLeaseManager(dbConfig);
		leaseManager_1.Initilize("consumer_group_1", "worker_instance_1", "sample_project", "sample_logstore");
		
		System.out.println(leaseManager_1.listLeases());
		
		MockLogHubClientAdapter clientAdapter = new MockLogHubClientAdapter();
		List<String> shards = new ArrayList<String>();
		for (int i = 0 ; i < 10; i++)
		{
			shards.add(String.valueOf(i));
		}
		clientAdapter.setShard(shards);
		//SLSClient loghubClient = new SLSClient("10.101.214.153:6001","a7zan0ywbuE794dm", "wxq6YGQ4csLRkCvFeE0HJvZA4oR7A6");
		//String project = "e2eproject1432909423";
		//String stream = "stream_for_loghub_client";
		//LogHubClientAdapter clientAdapter = new LogHubClientAdapter(loghubClient, project, stream);
		
		LogHubLeaseCoordinator coordinator_1 = new LogHubLeaseCoordinator(
				clientAdapter, leaseManager_1, "instance_1", 2000);

		coordinator_1.start();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		
		MySqlLogHubLeaseManager leaseManager_2 = new MySqlLogHubLeaseManager(dbConfig);
		leaseManager_2.Initilize("consumer_group_1", "worker_instance_2", "sample_project", "sample_logstore");
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		
		LogHubLeaseCoordinator coordinator_2 = new LogHubLeaseCoordinator(
				clientAdapter, leaseManager_2, "instance_2", 2000);
		coordinator_2.start();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

		for (int i = 0; i < 30; i++) {
			Map<String, LogHubLease> leases_1 = coordinator_1.getAllHeldLease();
			Map<String, LogHubLease> leases_2 = coordinator_2.getAllHeldLease();
			System.out.println("instance_1:" + leases_1.keySet().toString());
			System.out.println("instance_2:" + leases_2.keySet().toString());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			if (i  == 10)
			{
				coordinator_1.stop();
			}
			if( i == 20)
			{
				coordinator_2.stop();
			}
		}


	}

}

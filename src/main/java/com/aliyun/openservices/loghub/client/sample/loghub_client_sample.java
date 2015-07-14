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
		MySqlLogHubLeaseManager leaseManager = new MySqlLogHubLeaseManager(
				"conume_g_1" , "sig_1", dbConfig);
		leaseManager.Initilize();
		
		System.out.println(leaseManager.listLeases());
		
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
		
		leaseManager.registerWorker("instance_1");
		LogHubLeaseCoordinator coordinator_1 = new LogHubLeaseCoordinator(
				clientAdapter, leaseManager, "instance_1", 2000);

		coordinator_1.start();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		Map<String, LogHubLease> leases = coordinator_1.getAllHeldLease();
		//	assertEquals(leases.size(), 10);
		leaseManager.registerWorker("instance_2");
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		leases = coordinator_1.getAllHeldLease();
		//assertEquals(leases.size(), 10);
	
		
		LogHubLeaseCoordinator coordinator_2 = new LogHubLeaseCoordinator(
				clientAdapter, leaseManager, "instance_2", 2000);
		coordinator_2.start();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

		LogHubLeaseCoordinator coordinator_3 = new LogHubLeaseCoordinator(
				clientAdapter, leaseManager, "instance_3", 2000);
		coordinator_3.start();
		for (int i = 0; i < 30; i++) {
			Map<String, LogHubLease> leases_1 = coordinator_1.getAllHeldLease();
			Map<String, LogHubLease> leases_2 = coordinator_2.getAllHeldLease();
			Map<String, LogHubLease> leases_3 = coordinator_3.getAllHeldLease();
			System.out.println("instance_1:" + leases_1.keySet().toString());
			System.out.println("instance_2:" + leases_2.keySet().toString());
			System.out.println("instance_3:" + leases_3.keySet().toString());
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
		coordinator_3.stop();


	}

}

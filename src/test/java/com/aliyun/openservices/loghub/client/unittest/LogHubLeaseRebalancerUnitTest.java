package com.aliyun.openservices.loghub.client.unittest;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.aliyun.openservices.loghub.LogHubClient;
import com.aliyun.openservices.loghub.client.LogHubClientAdapter;
import com.aliyun.openservices.loghub.client.config.LogHubClientDbConfig;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;
import com.aliyun.openservices.loghub.client.lease.LogHubLeaseCoordinator;
import com.aliyun.openservices.loghub.client.lease.impl.LogHubLeaseRebalancer;
import com.aliyun.openservices.loghub.client.lease.impl.MySqlLogHubLeaseManager;

public class LogHubLeaseRebalancerUnitTest {
	@Test
	public void TestcreateNewLease() {
		List<String> allShards = new ArrayList<String>();
		List<String> existShards = new ArrayList<String>();
		List<String> expectedShards = new ArrayList<String>();
		for (int i = 0; i < 10; i++) {
			allShards.add(String.valueOf(i));
			expectedShards.add(String.valueOf(i));
		}
		checkCreate(allShards, existShards, expectedShards);
		
		allShards = new ArrayList<String>();
		existShards = new ArrayList<String>();
		expectedShards = new ArrayList<String>();
		for (int i = 0; i < 10; i++) {
			allShards.add(String.valueOf(i));
			if (i < 5)
			{
				existShards.add(String.valueOf(i));
			}
			else
			{
				expectedShards.add(String.valueOf(i));
			}
		}
		
		checkCreate(allShards, existShards, expectedShards);
		
		allShards = new ArrayList<String>();
		existShards = new ArrayList<String>();
		expectedShards = new ArrayList<String>();
		for (int i = 0; i < 10; i++) {
			allShards.add(String.valueOf(i));
			existShards.add(String.valueOf(i));
		}
		checkCreate(allShards, existShards, expectedShards);
	}

	public void checkCreate(List<String> allShards, List<String> existShards,
			List<String> expectedShards) {
		MockLogHubClientAdapter adapter = new MockLogHubClientAdapter();
		MockLogHubLeaseManager manager = new MockLogHubLeaseManager();
		LogHubLeaseRebalancer rebalancer = new LogHubLeaseRebalancer(adapter,
				manager, "instance", 1000);
		
		adapter.setShard(allShards);
		
		List<LogHubLease> mockExistLease = new ArrayList<LogHubLease>();
		for (String shardId : existShards) {
			mockExistLease.add(new LogHubLease(shardId, "", "", 0));
		}
		manager.setLeaseToList(mockExistLease);

		invokeMethod(rebalancer, "createNewLease", new Class[0], new Object[0]);
		
		List<String> toCreateShards = manager.listShardToCreate();
		checkListEqual(expectedShards, toCreateShards);
	}
	
	private Object invokeMethod(Object obj, String methodName, Class[] paramsClass, Object[] params)
	{
		boolean success = false;
		Object res = null;
		Method m;
		try {
			m = obj.getClass().getDeclaredMethod(methodName, paramsClass);

			m.setAccessible(true);
			res = m.invoke(obj, params);
			success = true;
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		assertEquals(methodName, success, true);
		return res;
	}
	
	private void checkListEqual(List<String> left, List<String> right) {
		assertNotNull(left);
		assertNotNull(right);
		assertEquals(left.size(), right.size());
		Set<String> leftset = new HashSet<String>(left);
		for(String obj :right)
		{
			assertTrue(leftset.contains(obj));
		}
		
	}
	
	@Test
	public void TestupdateAllLeases()
	{
		MockLogHubClientAdapter adapter = new MockLogHubClientAdapter();
		MockLogHubLeaseManager manager = new MockLogHubLeaseManager();
		LogHubLeaseRebalancer rebalancer = new LogHubLeaseRebalancer(adapter,
				manager, "instance", 1000);
		
		List<LogHubLease> leases = new ArrayList<LogHubLease>();
		Set<String> newCreateLeases = new HashSet<String>();
		Set<String> updatedLeases = new HashSet<String>();
		for (int i = 0 ; i < 10 ; i++)
		{
			String shardId = String.valueOf(i);
			LogHubLease lease = new LogHubLease(shardId, shardId, shardId , i);
			leases.add(lease);
			newCreateLeases.add(shardId);
		}
		manager.setLeaseToList(leases);
		updateAllLease(rebalancer, newCreateLeases, updatedLeases);
		
		leases = new ArrayList<LogHubLease>();
		newCreateLeases = new HashSet<String>();
		updatedLeases = new HashSet<String>();
		for (int i = 0; i < 20; i++) {
			String shardId = String.valueOf(i);
			LogHubLease lease = null;
			if (i < 5) {
				lease = new LogHubLease(shardId, shardId, shardId, i);
			} else if (i < 10) {
				lease = new LogHubLease(shardId, shardId, shardId, i + 1);
				updatedLeases.add(shardId);
			} else {
				lease = new LogHubLease(shardId, shardId, shardId, i + 1);
				newCreateLeases.add(shardId);
			}
			leases.add(lease);
		}
		manager.setLeaseToList(leases);
		updateAllLease(rebalancer, newCreateLeases, updatedLeases);
	}
	
	private void updateAllLease(LogHubLeaseRebalancer rebalance, 
			Set<String> newCreateLeases, Set<String> updatedLeases)
	{
		Map<String, Long> oldLeaseTime = new HashMap<String , Long>();
		Map<String, LogHubLease> seenLeases = (Map<String, LogHubLease>)(getPrivateObj(rebalance , "mAllSeenLeases"));
		for(String leaseKey : seenLeases.keySet())
		{
			oldLeaseTime.put(leaseKey, seenLeases.get(leaseKey).getLastUpdateTimeNanos());
		}
		invokeMethod(rebalance, "updateAllLeases", new Class[0], new Object[0]);
		seenLeases = (Map<String, LogHubLease>)(getPrivateObj(rebalance , "mAllSeenLeases"));
		for(Map.Entry<String, LogHubLease> entry :seenLeases.entrySet())
		{
			String leaseKey = entry.getKey();
			LogHubLease lease = entry.getValue();
			if (newCreateLeases.contains(leaseKey))
			{
				assertTrue(oldLeaseTime.containsKey(leaseKey) == false);
				newCreateLeases.remove(leaseKey);
			}
			else if (updatedLeases.contains(leaseKey))
			{
				assertTrue(oldLeaseTime.get(leaseKey).longValue() != (lease.getLastUpdateTimeNanos()));
			}
			else
			{
				assertTrue(leaseKey, oldLeaseTime.get(leaseKey).longValue() == (lease.getLastUpdateTimeNanos()));
			}
		}
		assertEquals(newCreateLeases.size() , 0);
	}
    private Object getPrivateObj(Object instance, String variableName)
    {
        Class targetClass = instance.getClass();
        Field field;
        try {
            field = targetClass.getDeclaredField(variableName);
            field.setAccessible(true);
            return field.get(instance);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    private void setPrivateOjb(Object instance, String variableName, Object value)
    {
    	Class targetClass = instance.getClass();
        Field field;
        try {
            field = targetClass.getDeclaredField(variableName);
            field.setAccessible(true);
            field.set(instance, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testGetExpiredLeases()
    {
    	MockLogHubClientAdapter adapter = new MockLogHubClientAdapter();
		MockLogHubLeaseManager manager = new MockLogHubLeaseManager();
		LogHubLeaseRebalancer rebalancer = new LogHubLeaseRebalancer(adapter,
				manager, "instance", 1000);
		HashMap<String, LogHubLease>  allLeases = (HashMap<String, LogHubLease>)(getPrivateObj(rebalancer, "mAllSeenLeases"));
		long curTimeNanos = System.nanoTime();
		LogHubLease expiredLease = new  LogHubLease("0", "0", "0" , 0);
		expiredLease.setLastUpdateTimeNaons(curTimeNanos - 1000 * 1000 * 1000);
		allLeases.put("0", expiredLease);
		LogHubLease not_expired_lease = new  LogHubLease("1", "1", "1" , 1);
		not_expired_lease.setLastUpdateTimeNaons(curTimeNanos);
		allLeases.put("1", not_expired_lease);
		List<LogHubLease> expiredLeases = (List<LogHubLease>)(invokeMethod(rebalancer, "getExpiredLeases" , new Class[0], new Object[0]));
		assertEquals(expiredLeases.size(), 1);
		assertEquals(expiredLeases.get(0), expiredLease);
    }
    
    
   @Test 
	public void TestComputeLeasesToTake_all() {
		List<LogHubLease> allLease = new ArrayList<LogHubLease>();
		List<LogHubLease> expiredLease = new ArrayList<LogHubLease>();
		String instanceName = "0";
		List<String> allInstance = new ArrayList<String>();
		Set<String> mustTakedLease = new HashSet<String>();
		Set<String> optinalTakeLeas = new HashSet<String>();
		int optionalTakeCount = 0;
		for( int i = 0 ; i < 10 ; i++)
		{
			String shardId = String.valueOf(i);
			LogHubLease lease = new LogHubLease(shardId, shardId, shardId, 0);
			allLease.add(lease);
			expiredLease.add(lease);
			mustTakedLease.add(shardId);
		}
		
		ComputeLeasesToTakeHelp(allLease, expiredLease, instanceName, 1000,
				allInstance, mustTakedLease, optinalTakeLeas, optionalTakeCount);
	}
   
	@Test
	public void TestComputeLeasesToTake_part_1() {
		List<LogHubLease> allLease = new ArrayList<LogHubLease>();
		List<LogHubLease> expiredLease = new ArrayList<LogHubLease>();
		String instanceName = "a";
		List<String> allInstance = new ArrayList<String>();
		Set<String> mustTakedLease = new HashSet<String>();
		Set<String> optinalTakeLeas = new HashSet<String>();
		int optionalTakeCount = 3;
		for (int i = 0; i < 10; i++) {
			String shardId = String.valueOf(i);

			LogHubLease lease = null;
			lease = new LogHubLease(shardId, "b", "b", 0);
			if (i < 2) {
				expiredLease.add(lease);
				mustTakedLease.add(shardId);
				expiredLease.add(lease);
			} else {
				lease.setLastUpdateTimeNaons(System.nanoTime());
				optinalTakeLeas.add(shardId);
			}
			allLease.add(lease);
		}

		ComputeLeasesToTakeHelp(allLease, expiredLease, instanceName, 10000,
				allInstance, mustTakedLease, optinalTakeLeas, optionalTakeCount);
	}
	
	@Test
	public void TestComputeLeasesToTake_part_2() {
		List<LogHubLease> allLease = new ArrayList<LogHubLease>();
		List<LogHubLease> expiredLease = new ArrayList<LogHubLease>();
		String instanceName = "a";
		List<String> allInstance = new ArrayList<String>();
		Set<String> mustTakedLease = new HashSet<String>();
		Set<String> optinalTakeLeas = new HashSet<String>();
		int optionalTakeCount = 3;
		for (int i = 0; i < 9 ; i++) {
			String shardId = String.valueOf(i);
			LogHubLease lease = null;
			if (i < 6)
			{
				lease = new LogHubLease(shardId, "b", "b", 0);
				optinalTakeLeas.add(shardId);
			}
			else
			{
				lease = new LogHubLease(shardId, "c", "c", 0);
			}
			allLease.add(lease);
		}

		ComputeLeasesToTakeHelp(allLease, expiredLease, instanceName, 10000,
				allInstance, mustTakedLease, optinalTakeLeas, optionalTakeCount);
	}
	
	public void TestComputeLeasesToTake_part_3() {
		List<LogHubLease> allLease = new ArrayList<LogHubLease>();
		List<LogHubLease> expiredLease = new ArrayList<LogHubLease>();
		String instanceName = "a";
		List<String> allInstance = new ArrayList<String>();
		allInstance.add("c");
		Set<String> mustTakedLease = new HashSet<String>();
		Set<String> optinalTakeLeas = new HashSet<String>();
		int optionalTakeCount = 3;
		for (int i = 0; i < 9 ; i++) {
			String shardId = String.valueOf(i);
			LogHubLease lease = null;
			lease = new LogHubLease(shardId, "b", "b", 0);
			optinalTakeLeas.add(shardId);
			allLease.add(lease);
		}

		ComputeLeasesToTakeHelp(allLease, expiredLease, instanceName, 10000,
				allInstance, mustTakedLease, optinalTakeLeas, optionalTakeCount);
	}
	
	public void TestComputeLeasesToTake_part_4() {
		List<LogHubLease> allLease = new ArrayList<LogHubLease>();
		List<LogHubLease> expiredLease = new ArrayList<LogHubLease>();
		String instanceName = "a";
		List<String> allInstance = new ArrayList<String>();
		Set<String> mustTakedLease = new HashSet<String>();
		Set<String> optinalTakeLeas = new HashSet<String>();
		int optionalTakeCount = 1;
		LogHubLease lease = null;
		
		lease = new LogHubLease("0", "b", "b", 0);
		allLease.add(lease);
		optinalTakeLeas.add("0");
		lease = new LogHubLease("1", "b", "b", 0);
		allLease.add(lease);
		optinalTakeLeas.add("1");
		lease = new LogHubLease("2", "c", "c", 0);
		allLease.add(lease);
		

		ComputeLeasesToTakeHelp(allLease, expiredLease, instanceName, 10000,
				allInstance, mustTakedLease, optinalTakeLeas, optionalTakeCount);
	}
	
	
   
	private void ComputeLeasesToTakeHelp(List<LogHubLease> allLeases,
			List<LogHubLease> expiredLease, String instanceName,
			long leaseDurationMillis, List<String> allInstance,
			Set<String> mustTakedLease, Set<String> optinalTakeLeas,
			int optionalTakeCount) {
		MockLogHubClientAdapter adapter = new MockLogHubClientAdapter();
		MockLogHubLeaseManager manager = new MockLogHubLeaseManager();
		LogHubLeaseRebalancer rebalancer = new LogHubLeaseRebalancer(adapter,
				manager, instanceName, leaseDurationMillis);
		HashMap<String, LogHubLease> seenLeases = (HashMap<String, LogHubLease>) (getPrivateObj(
				rebalancer, "mAllSeenLeases"));
		for (LogHubLease lease : allLeases) {
			seenLeases.put(lease.getLeaseKey(), lease);
		}
		manager.setInstance(allInstance);
		
		Object[] params = new Object[] { expiredLease };
		Set<LogHubLease> toTakeLease = (Set<LogHubLease>) (invokeMethod(
				rebalancer, "computeLeasesToTake", new Class[] { List.class },
				params));
		
		assertEquals(toTakeLease.size(), optionalTakeCount + mustTakedLease.size());
		
		Set<String> toTakeLeaseKeys = new HashSet<String>();
		int must_take_count = 0;
		int option_take_count = 0;
		for(LogHubLease lease : toTakeLease)
		{
			String leaseKey = lease.getLeaseKey();
			if (mustTakedLease.contains(leaseKey))
			{
				must_take_count += 1;
			}
			else if (optinalTakeLeas.contains(leaseKey))
			{
				option_take_count += 1;
			}
			else
			{
				assertTrue("The lease key should not take:" + leaseKey, false);
			}	
		}
		assertEquals(must_take_count, mustTakedLease.size());
		assertEquals(option_take_count, optionalTakeCount);
	}
	
	public void TestLogHubClient()
	{
		LogHubClient loghubClient = new LogHubClient("10.101.214.153", 60001, "a7zan0ywbuE794dm", "wxq6YGQ4csLRkCvFeE0HJvZA4oR7A6");
	
		String project = "loghub-client-worker-test";
		String stream = "test_2_shards";
		
		
/*		try {
			loghubClient.createLogStream(project, new LogStreamResource(stream, 
					new LogStreamDetailResource(new LogStreamLifeCycleResource(), new LogStreamShardResource(2))));
		} catch (LogHubException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LogHubClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */

		
		
		LogHubClientAdapter adapter = new LogHubClientAdapter(loghubClient, project, stream);
		System.out.println(adapter.listShard().toString());
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				"10.101.172.22", 3306, "scmc", "apsara", "123456",
				"loghub_worker", "loghub_lease");
		
		LogHubConfig config = new  LogHubConfig("consume_group_ut_1", "instance_1", "10.101.214.153", 60001, 
				"e2eproject1432909423", "e2elogstream1432909423",
				"a7zan0ywbuE794dm", "wxq6YGQ4csLRkCvFeE0HJvZA4oR7A6", dbConfig, LogHubCursorPosition.END_CURSOR);
	}
	//@Test
	public void xxxTestSample() throws LogHubLeaseException {
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				"10.101.172.22", 3306, "scmc", "apsara", "123456",
				"loghub_worker", "loghub_lease");
		MySqlLogHubLeaseManager leaseManager = new MySqlLogHubLeaseManager(
				"conume_g_1" , "sig_11", dbConfig);
		leaseManager.Initilize();
		
		
		System.out.println(leaseManager.listLeases());
		
		
		
		MockLogHubClientAdapter clientAdapter = new MockLogHubClientAdapter();
		List<String> shards = new ArrayList<String>();
		for (int i = 0 ; i < 10; i++)
		{
			shards.add(String.valueOf(i));
		}
		clientAdapter.setShard(shards);
		LogHubClient loghubClient = new LogHubClient("10.101.214.153", 60001, "a7zan0ywbuE794dm", "wxq6YGQ4csLRkCvFeE0HJvZA4oR7A6");
		String project = "e2eproject1432909423";
		String stream = "stream_for_loghub_client";
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
			System.out.println(leases_1.keySet().toString());
			System.out.println(leases_2.keySet().toString());
			System.out.println(leases_3.keySet().toString());
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

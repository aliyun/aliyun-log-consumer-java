package com.aliyun.openservices.loghub.client.lease.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.client.LogHubClientAdapter;
import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseRebalancer;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;

public class LogHubLeaseRebalancer implements ILogHubLeaseRebalancer{
	private final ILogHubLeaseManager mLeaseManager;
	private final String mInstanceName;
	private final long mLeaseDuration_nanos;
	private final Map<String, LogHubLease> mAllSeenLeases = new HashMap<String, LogHubLease>();
	private final LogHubClientAdapter mLogHubClientAdapter;
	private boolean mFirstTaken = true;
	private long mLastCheckNewShardTime_ms = 0;
	public static final long LIST_SHARD_INTERVAL_MS = 60 * 1000;
	protected Logger logger = Logger.getLogger(this.getClass());

	public LogHubLeaseRebalancer(LogHubClientAdapter clientAdpter, ILogHubLeaseManager leaseManager, String instanceName, long leaseDurationMillis)
	{
		this.mLogHubClientAdapter = clientAdpter;
		this.mLeaseManager = leaseManager;
		this.mInstanceName = instanceName;
		this.mLeaseDuration_nanos = leaseDurationMillis * 1000000L;
	}
	public boolean initialize()
	{
		boolean res = createNewLease();
		mLastCheckNewShardTime_ms = System.currentTimeMillis();
		return res;
	}
	
	private boolean createNewLease() {
		try {
			List<String> shards = mLogHubClientAdapter.listShard();
			List<LogHubLease> shardLeases;
			shardLeases = mLeaseManager.listLeases();

			if (shards == null || shardLeases == null) {
				return false;
			}
			Set<String> withLeaseShards = new HashSet<String>();
			for (LogHubLease lease : shardLeases) {
				withLeaseShards.add(lease.getLeaseKey());
			}
			List<String> toCreateShards = new ArrayList<String>();
			for (String shard_id : shards) {
				if (withLeaseShards.contains(shard_id) == false) {
					toCreateShards.add(shard_id);
				}
			}

			return mLeaseManager.createLeaseForShards(toCreateShards);
		} catch (LogHubLeaseException e) {
			logger.error("Failed to create new lease", e);
			return false;
		}
	}

	private void newShardCreateCheck() {
		long curTime = System.currentTimeMillis();
		if (curTime > mLastCheckNewShardTime_ms + LIST_SHARD_INTERVAL_MS) {
			createNewLease();
			mLastCheckNewShardTime_ms = System.currentTimeMillis();
		}
	}

	public List<LogHubLease> takeLeases() throws LogHubLeaseException {
		newShardCreateCheck();
		updateAllLeases();

		List<LogHubLease> expiredLeases = getExpiredLeases();
		Set<LogHubLease> leasesToTake = computeLeasesToTake(expiredLeases);
		List<LogHubLease> takenLeases = new ArrayList<LogHubLease>();
		for (LogHubLease lease : leasesToTake) {

			String consumer = null;
			if (expiredLeases.contains(lease)) {
				consumer = mInstanceName;
			}
			long curTime = System.nanoTime();

			try {
				boolean res = mLeaseManager.takeLease(lease,
						this.mInstanceName, consumer);

				if (res) {
					lease.setLastUpdateTimeNaons(curTime);
					lease.setLeaseOwner(this.mInstanceName);
					lease.setLeaseConsumer(consumer);
					if (consumer == null) {
						lease.setConsumerStartTimeNanos(System.nanoTime()
								+ this.mLeaseDuration_nanos);
					} else {
						lease.setConsumerStartTimeNanos(curTime);
					}
					takenLeases.add(lease);
				}
				logger.info("instance:" + this.mInstanceName
						+ ";try to take lease key:" + lease.getLeaseKey()
						+ ";lease id:" + String.valueOf(lease.getLeaseId())
						+ ";with owner:" + mInstanceName + ";with consumer"
						+ consumer + ";result:" + String.valueOf(res));
			} catch (LogHubLeaseException e) {
				logger.error("Failed to take lease:" + lease.toString(), e);
			}
		}

		return takenLeases;
	}
	
	// return instance_name => instance_held_lease
	private Map<String, List<LogHubLease>> computeInstanceLeases(
			List<LogHubLease> expiredLeases) throws LogHubLeaseException {
		Map<String, List<LogHubLease>> instanceLeases = new HashMap<String, List<LogHubLease>>();
		for (LogHubLease lease : mAllSeenLeases.values()) {
			if (expiredLeases.contains(lease) == false) {
				String leaseOwner = lease.getLeaseOwner();
				if (instanceLeases.containsKey(leaseOwner)) {
					instanceLeases.get(leaseOwner).add(lease);
				} else {
					List<LogHubLease> held_leases = new LinkedList<LogHubLease>();
					held_leases.add(lease);
					instanceLeases.put(leaseOwner, held_leases);
				}
			}
		}
		List<String> allInstances = new ArrayList<String>();
		if (mFirstTaken) {
			allInstances = mLeaseManager.listNewCreatedInstance();
			mFirstTaken = false;
		}

		allInstances.add(mInstanceName);

		for (String instance : allInstances) {
			if (instanceLeases.containsKey(instance) == false) {
				instanceLeases.put(instance, new LinkedList<LogHubLease>());
			}
		}

		return instanceLeases;
	}
    
	class CountEntry
	{
		private Integer mCount;
		private String mKey;
		public CountEntry(Integer count, String key)
		{
			this.mCount = count;
			this.mKey = key;
		}
		public Integer getCount()
		{
			return mCount;
		}
		public String getKey()
		{
			return mKey;
		}
		
	};
	
	class CountEntryComparator implements Comparator<CountEntry>
	{
		public int compare(CountEntry o1, CountEntry o2) {
			return 0 - o1.getCount().compareTo(o2.getCount());
		}
	}
	
	private void getRandomItem(Set<LogHubLease> des, List<LogHubLease> from, int count)
	{

        // Shuffle expiredLeases so workers don't all try to contend for the same leases.
        Collections.shuffle(from);
        
        Iterator<LogHubLease> it = from.iterator();
        while(it.hasNext() && count > 0)
        {
        	des.add(it.next());
        	count--;
        }
	}
	
	private Set<LogHubLease> computeLeasesToTake(List<LogHubLease> expiredLeases) throws LogHubLeaseException {
		Map<String, List<LogHubLease>> instanceLeases = computeInstanceLeases(expiredLeases);
		logger.info("instance:" + this.mInstanceName + " Find instance count:"
				+ String.valueOf(instanceLeases.size()) + ",instances:"
				+ instanceLeases.keySet().toString());
		Set<LogHubLease> leasesToTake = new HashSet<LogHubLease>();

		int numLeases = this.mAllSeenLeases.size();
		int numWorkers = instanceLeases.size();

		if (numLeases == 0) {
			return leasesToTake;
		}

		int target = numLeases / numWorkers + (numLeases % numWorkers == 0 ? 0 : 1);
		int myCount = instanceLeases.get(this.mInstanceName).size();
		int numLeasesToReachTarget = target - myCount;

		if (numLeasesToReachTarget <= 0) {
			// If we don't need anything, return the empty set.
			return leasesToTake;
		}

		getRandomItem(leasesToTake, expiredLeases, numLeasesToReachTarget);
		numLeasesToReachTarget -= leasesToTake.size();

		if (numLeasesToReachTarget > 0) {
			int mode = numLeases % numWorkers;
			if (mode > 0 && getCountLargerOrEqual(instanceLeases, target) >= mode)
			{
				numLeasesToReachTarget--;
			}
			
			List<CountEntry> to_steal_instance = new ArrayList<CountEntry>();
			for (Map.Entry<String, List<LogHubLease>> entry : instanceLeases.entrySet()) {
				int held_leases_size = entry.getValue().size();
				to_steal_instance.add(new CountEntry(held_leases_size, entry.getKey()));
			}
			
			Map<String, Integer> steal_targets = computeStealTarget(
					to_steal_instance, numLeasesToReachTarget);
			
			for (Map.Entry<String, Integer> entry : steal_targets.entrySet()) {
				getRandomItem(leasesToTake, instanceLeases.get(entry.getKey()),
						entry.getValue());
			}
		}
	

		return leasesToTake;
	}

	private int getCountLargerOrEqual(Map<String, List<LogHubLease>> instances,
			int target) {
		int res = 0;
		for (Map.Entry<String, List<LogHubLease>> entry : instances.entrySet()) {
			if (entry.getValue().size() >= target) {
				res++;
			}
		}
		return res;
	}
	private Map<String, Integer> computeStealTarget(
			List<CountEntry> to_steal_instance, int to_steal_count) {
		Collections.sort(to_steal_instance, new CountEntryComparator());
		Map<String, Integer> steal_targets = new HashMap<String, Integer>();
		int total_count = 0;
		int index = 0;
		for (index = 0; index < to_steal_instance.size(); index++) {
			total_count += to_steal_instance.get(index).getCount();
			if (index == to_steal_instance.size() - 1
					|| (total_count - to_steal_count) / (index + 1) >= to_steal_instance
							.get(index + 1).getCount()) {
				break;
			}
		}
		int to_steal_instances = index + 1;
		int left_count = total_count - to_steal_count;
		int left_avg = left_count / to_steal_instances;
		int left_mode = left_count % to_steal_instances;
		for (int i = 0; i <= index && to_steal_count > 0; i++) {
			CountEntry entry = to_steal_instance.get(i);
			int steal_count = entry.getCount() - left_avg;
			if (left_mode > 0) {
				left_mode--;
				steal_count--;
			}
			int real_steal_count = Math.min(steal_count, to_steal_count);
			if (real_steal_count > 0) {
				steal_targets.put(entry.getKey(), real_steal_count);
				to_steal_count -= real_steal_count;
			}
		}
		return steal_targets;
	}
	 
	 
	private List<LogHubLease> getExpiredLeases() {
		long curTime = System.nanoTime();
		List<LogHubLease> expiredLeases = new ArrayList<LogHubLease>();
		for (LogHubLease lease : mAllSeenLeases.values()) {
			if (lease.isExpired(mLeaseDuration_nanos, curTime)) {
				expiredLeases.add(lease);
				logger.info("Find a timeout lease:" + lease.getLeaseKey());
			}
		}
		return expiredLeases;
	}
	
    private void updateAllLeases() throws LogHubLeaseException {
    	logger.debug("instance:" + this.mInstanceName + " Begin to fresh lease from outside system");
    	List<LogHubLease> freshList =  mLeaseManager.listLeases();

		long scan_time = System.nanoTime();

		Set<String> notUpdated = new HashSet<String>(mAllSeenLeases.keySet());

		for (LogHubLease lease : freshList) {
			String leaseKey = lease.getLeaseKey();

			LogHubLease oldLease = mAllSeenLeases.get(leaseKey);
			mAllSeenLeases.put(leaseKey, lease);
			notUpdated.remove(leaseKey);

			if (oldLease != null) {
				
				logger.debug("instance:" + this.mInstanceName + " Find a old lease, with lease id:" + lease.toString());
				if (oldLease.getLeaseId() == lease.getLeaseId()) {
					lease.setLastUpdateTimeNaons(oldLease
							.getLastUpdateTimeNanos());
				} else {
					lease.setLastUpdateTimeNaons(scan_time);
				}
			} else {
				logger.debug("instance:" + this.mInstanceName + " Find a new lease, with lease id:" + lease.toString());
				if (lease.getLeaseOwner() == null) {
					lease.setLastUpdateTimeNaons(0L);
				} else {
					lease.setLastUpdateTimeNaons(scan_time);
				}
			}
		}
	}
    


}

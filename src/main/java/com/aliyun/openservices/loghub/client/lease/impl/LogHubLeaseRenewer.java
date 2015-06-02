package com.aliyun.openservices.loghub.client.lease.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;

import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseRenewer;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;

public class LogHubLeaseRenewer implements ILogHubLeaseRenewer {
	private final ILogHubLeaseManager mLeaseManager;
	private final String mInstanceName;
	private final long mLeaseDuration_nanos;
	private final ConcurrentNavigableMap<String, LogHubLease> mHeldLeases= new ConcurrentSkipListMap<String, LogHubLease>();
	protected Logger logger = Logger.getLogger(this.getClass());
	
	public LogHubLeaseRenewer(ILogHubLeaseManager leaseManager, String instanceName, long leaseDurationMillis)
	{
		this.mLeaseManager = leaseManager;
		this.mInstanceName = instanceName;
		this.mLeaseDuration_nanos = leaseDurationMillis * 1000000L;
	}
	
	public void initialize() {
		List<LogHubLease> leases;
		try {
			leases = mLeaseManager.listLeases();
		} catch (LogHubLeaseException e) {
			logger.error("Failed to list exist shards from lease manager");
			return;
		}
		List<LogHubLease> re_held_leases = new ArrayList<LogHubLease>();
		long curTime = System.nanoTime();
		for (LogHubLease lease : leases) {
			if (this.mInstanceName.equals(lease.getLeaseOwner())) {
				if (lease.isConsumerHoldLease()) {
					lease.setConsumerStartTimeNanos(curTime);
				} else {
					lease.setConsumerStartTimeNanos(curTime
							+ mLeaseDuration_nanos);
				}
				if (renewLease(lease)) {
					re_held_leases.add(lease);
				}
			}
		}
		addLeasesToRenew(re_held_leases);
	}

	public void renewLeases() {
/*		for (LogHubLease lease : this.mHeldLeases.descendingMap().values()) {
			renewLease(lease);
		}*/
		Map<String, LogHubLease> all_leases = new HashMap<String, LogHubLease>();
		Set<String> toRenewConsumerShards = new HashSet<String>();
		long curTime = System.nanoTime();
		for (LogHubLease lease : mHeldLeases.descendingMap().values()) {
			LogHubLease copy_lease = null;
			String shardId = lease.getLeaseKey();
			synchronized (lease) {
				copy_lease = lease.copy();
			}
			all_leases.put(shardId, copy_lease);
			if (curTime > lease.getConsumerStartTimeNanos()
					&& lease.isConsumerHoldLease() == false) {
				toRenewConsumerShards.add(shardId);
			}
		}
		Set<String> renewSuccessShards = new HashSet<String>();
		try {
			mLeaseManager.batchRenewLease(all_leases, toRenewConsumerShards,
					renewSuccessShards);
		} catch (LogHubLeaseException e) {
			logger.error("Failed to renew all release", e);
		}
		for (Map.Entry<String, LogHubLease> entry : all_leases.entrySet()) {
			String shardId = entry.getKey();
			LogHubLease lease = entry.getValue();
			if (renewSuccessShards.contains(shardId)) {
				lease.setLastUpdateTimeNaons(curTime);
				mHeldLeases.put(shardId, lease);
			} else if (lease.isExpired(this.mLeaseDuration_nanos,
					System.nanoTime())) {
				this.mHeldLeases.remove(lease.getLeaseKey());
			}
		}
		logger.info("instance:"
				+ mInstanceName
				+ ";renew release,all:"
				+ all_leases.keySet().toString()
				+ ";success:"
				+ renewSuccessShards.toString()
				+ "total renew time(ms)"
				+ String.valueOf((System.nanoTime() - curTime) / 1000.0 / 1000.0));
	}
	
	private boolean renewLease(LogHubLease lease) {
		boolean success = false;
		try {
			synchronized (lease) {
				long curTime = System.nanoTime();
				boolean update_consumer = false;
				if (curTime > lease.getConsumerStartTimeNanos()
						&& lease.isConsumerHoldLease() == false) {
					update_consumer = true;
				}
				if (mLeaseManager.renewLease(lease, update_consumer)) {
					lease.setLastUpdateTimeNaons(curTime);
					if (update_consumer) {
						lease.setLeaseConsumer(lease.getLeaseOwner());
					}
					success = true;
				}
			}
			if (success == false && 
				lease.isExpired(this.mLeaseDuration_nanos, System.nanoTime())) {
				this.mHeldLeases.remove(lease.getLeaseKey());
				logger.warn("instance:" + this.mInstanceName
						+ " Failed renew lease:" + lease.toString());
			} else {
				logger.info("instance:" + this.mInstanceName
						+ " Success to renew lease:" + lease.toString());
			}
		} catch (LogHubLeaseException e) {
			logger.error("Failed to renew lease instance:" + this.mInstanceName
					+ " Failed renew lease:" + lease.toString(), e);

		}
		return success;
	}
	
    private LogHubLease getCopyOfHeldLease(String leaseKey, long now) {
    	LogHubLease lease = this.mHeldLeases.get(leaseKey);
        if (lease == null) {
            return null;
        } else {
        	LogHubLease copy = null;
            synchronized (lease) {
                copy = lease.copy();
            }
            if (copy.isExpired(this.mLeaseDuration_nanos, now)) {
                return null;
            } else {
                return copy;
            }
        }
    }

	public Map<String, LogHubLease> getHeldLeases() {
        Map<String, LogHubLease> result = new HashMap<String, LogHubLease>();
        long now = System.nanoTime();

        for (String leaseKey : mHeldLeases.keySet()) {
        	LogHubLease copy = getCopyOfHeldLease(leaseKey, now);
            if (copy != null) {
                result.put(copy.getLeaseKey(), copy);
            }
        }
  
		return result;
	}

	public LogHubLease getHeldLeases(String leaseKey) {
		return getCopyOfHeldLease(leaseKey, System.nanoTime());
	}

	public void addLeasesToRenew(Collection<LogHubLease> newLeases) {
		for(LogHubLease lease : newLeases)
		{
			if (lease.getLastUpdateTimeNanos() != 0)
			{
				this.mHeldLeases.put(lease.getLeaseKey(), lease.copy());
			}
		}
	}
	
	public void clearHeldLeases()
	{
		this.mHeldLeases.clear();
	}

}

package com.aliyun.openservices.loghub.client.unittest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;
import com.aliyun.openservices.loghub.client.lease.ILogHubLeaseManager;
import com.aliyun.openservices.loghub.client.lease.LogHubLease;

public class MockLogHubLeaseManager implements ILogHubLeaseManager{

	private List<LogHubLease> mLeaseToList = new ArrayList<LogHubLease>();
	private List<String> mShardToCreate = new ArrayList<String>();
	private List<String> mInstance = new ArrayList<String>();
	@Override
	public boolean Initilize() throws LogHubLeaseException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> listNewCreatedInstance() throws LogHubLeaseException {
		// TODO Auto-generated method stub
		return mInstance;
	}
	public void setInstance(List<String> instance)
	{
		mInstance = instance;
	}


	public void setLeaseToList(List<LogHubLease> lease)
	{
		mLeaseToList = lease;
	}
	public List<LogHubLease> listLeases() throws LogHubLeaseException {
		return mLeaseToList;
	}

	@Override
	public boolean createLeaseForShards(List<String> lease)
			throws LogHubLeaseException {
		mShardToCreate = lease;
		return true;
	}
	public List<String> listShardToCreate()
	{
		return mShardToCreate;
	}

	public void registerWorker(String workerName) throws LogHubLeaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean renewLease(LogHubLease lease, boolean update_consumer)
			throws LogHubLeaseException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void batchRenewLease(Map<String, LogHubLease> leases,
			Set<String> toRenewConsumerShards, Set<String> renewSuccessShards)
			throws LogHubLeaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean takeLease(LogHubLease lease, String leaseOnwer,
			String leaseConsumer) throws LogHubLeaseException {
		lease.setLeaseId(lease.getLeaseId() + 1);
		lease.setLeaseOwner(leaseOnwer);
		if (leaseConsumer != null) {
			lease.setLeaseConsumer(leaseConsumer);
		}
		return true;
	}

	@Override
	public boolean updateCheckPoint(String shardId, String checkPoint,
			String consumerOwner) throws LogHubLeaseException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getCheckPoint(String shardId) throws LogHubLeaseException {
		// TODO Auto-generated method stub
		return null;
	}

}

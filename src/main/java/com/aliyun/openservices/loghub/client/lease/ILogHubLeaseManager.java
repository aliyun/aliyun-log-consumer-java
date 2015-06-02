package com.aliyun.openservices.loghub.client.lease;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;

public interface ILogHubLeaseManager {

	public boolean Initilize() throws LogHubLeaseException;

	public List<String> listNewCreatedInstance() throws LogHubLeaseException;

	public List<LogHubLease> listLeases() throws LogHubLeaseException;

	public boolean createLeaseForShards(List<String> lease)
			throws LogHubLeaseException;

	public void registerWorker(String workerName) throws LogHubLeaseException;

	// if update_consumer is set to true, user lease.mLeaseOwner as consumer,
	// and save it to back end
	public boolean renewLease(LogHubLease lease, boolean update_consumer)
			throws LogHubLeaseException;
	
	
	public void batchRenewLease(Map<String, LogHubLease> leases, Set<String> toRenewConsumerShards, 
			Set<String> renewSuccessShards) throws LogHubLeaseException;
	
	// if leaseConsumer is not null, save the consumer into the backend,
	// otherwise , don't update consumer
	public boolean takeLease(LogHubLease lease, String leaseOnwer,
			String leaseConsumer) throws LogHubLeaseException;

	// persistent the check point only if the consumerOnwer is same( in
	// persistent system)
	public boolean updateCheckPoint(String shardId, String checkPoint,
			String consumerOwner) throws LogHubLeaseException;

	public String getCheckPoint(String shardId) throws LogHubLeaseException;

}

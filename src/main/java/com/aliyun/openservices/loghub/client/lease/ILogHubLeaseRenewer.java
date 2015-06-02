package com.aliyun.openservices.loghub.client.lease;

import java.util.Collection;
import java.util.Map;

public interface ILogHubLeaseRenewer {
	
	public void initialize();
	public void renewLeases();
	
	public Map<String, LogHubLease> getHeldLeases();
	
	
	public LogHubLease getHeldLeases(String leaseKey);
	
	public void addLeasesToRenew(Collection<LogHubLease> newLeases);
	
	public void clearHeldLeases();

}

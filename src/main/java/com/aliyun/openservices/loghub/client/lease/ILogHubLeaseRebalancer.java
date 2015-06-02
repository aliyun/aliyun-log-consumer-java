package com.aliyun.openservices.loghub.client.lease;

import java.util.List;

import com.aliyun.openservices.loghub.client.excpetions.LogHubLeaseException;

public interface ILogHubLeaseRebalancer {
	public boolean initialize();
	public List<LogHubLease> takeLeases() throws LogHubLeaseException;

}

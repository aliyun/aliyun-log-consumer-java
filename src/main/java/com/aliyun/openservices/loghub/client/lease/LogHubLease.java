package com.aliyun.openservices.loghub.client.lease;

public class LogHubLease {

	private String mLeaseKey;
	private String mLeaseOwner;
	private String mLeaseConsumer;
	private long mLeaseId = 0L;
	private long mLeaseConsumerStartTime_nanos = 0;
	private long mLeaseLastUpdateTime_nanos = 0;

	/**
	 * Constructor.
	 */
	public LogHubLease(String leaseKey, String leaseOwner,
			String leaseConsumer, long leaseId) {
		this.mLeaseKey = leaseKey;
		this.mLeaseOwner = leaseOwner;
		this.mLeaseConsumer = leaseConsumer;
		this.mLeaseId = leaseId;
	}


	public LogHubLease(LogHubLease lease) {
		this.mLeaseKey = lease.mLeaseKey;
		this.mLeaseOwner = lease.mLeaseOwner;
		this.mLeaseConsumer = lease.mLeaseConsumer;
		this.mLeaseId = lease.mLeaseId;
		this.mLeaseConsumerStartTime_nanos = lease.mLeaseConsumerStartTime_nanos;
		this.mLeaseLastUpdateTime_nanos = lease.mLeaseLastUpdateTime_nanos;

	}

	public void setLeaseOwner(String leaseOwner)
	{
		this.mLeaseOwner = leaseOwner;
	}
	
	public String getLeaseOwner() {
		return this.mLeaseOwner;
	}

	public String getLeaseConsumer() {
		return this.mLeaseConsumer;
	}

	public void setLeaseConsumer(String consumer) {
		this.mLeaseConsumer = consumer;
	}

	public long getLeaseId() {
		return mLeaseId;
	}
	public void setLeaseId(long leaseId)
	{
		mLeaseId = leaseId;
	}
	public void setLastUpdateTimeNaons(long updateTimeNanos) {
		this.mLeaseLastUpdateTime_nanos = updateTimeNanos;
	}
	public long getLastUpdateTimeNanos() {
		return this.mLeaseLastUpdateTime_nanos;
	}

	public void setConsumerStartTimeNanos(long consumerStartTimeNano) {
		this.mLeaseConsumerStartTime_nanos = consumerStartTimeNano;
	}
	public long getConsumerStartTimeNanos()
	{
		return this.mLeaseConsumerStartTime_nanos;
	}

	public String getLeaseKey() {
		return mLeaseKey;
	}
	public boolean isConsumerHoldLease()
	{
		return this.mLeaseOwner.equals(this.mLeaseConsumer);
	}
	
	public void ConsumerHoldLease()
	{
		this.mLeaseConsumer = this.mLeaseOwner;
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (obj instanceof LogHubLease) {
			LogHubLease lease = (LogHubLease) (obj);
			return StringEqual(mLeaseKey, lease.mLeaseKey)
					&& StringEqual(mLeaseOwner, lease.mLeaseOwner)
					&& StringEqual(mLeaseConsumer, lease.mLeaseConsumer)
					&& mLeaseId == lease.mLeaseId;
		}
		return false;
	}
	
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((mLeaseKey == null) ? 0 : mLeaseKey.hashCode());
        result = prime * result + ((mLeaseOwner == null) ? 0 : mLeaseOwner.hashCode());
        result = prime * result + ((mLeaseConsumer == null) ? 0 : mLeaseConsumer.hashCode());
        result = prime * result + (int)(mLeaseId);
        return result;
    }

    
	private boolean StringEqual(String left, String right)
	{
		if (left == null)
		{
			return right == null;
		}
		else
		{
			return left.equals(right);
		}
	}

	public String toString() {
		return "key:" + mLeaseKey + ";owner:" + mLeaseOwner + ";consumer:"
				+ mLeaseConsumer + ";lease_id:" + String.valueOf(mLeaseId)
				+ ";consumer_start_time:"
				+ String.valueOf(mLeaseConsumerStartTime_nanos)
				+ ";updatetime:" + String.valueOf(mLeaseLastUpdateTime_nanos);
	}

	public LogHubLease copy() {
		return new LogHubLease(this);
	}

	public boolean isExpired(long leaseDurationNanos, long checkTimeNanos) {
		long time_interval = checkTimeNanos - this.mLeaseLastUpdateTime_nanos;
		if (time_interval < 0 || time_interval > leaseDurationNanos) {
			return true;
		}
		return false;
	}
	

}

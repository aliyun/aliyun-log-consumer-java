package com.aliyun.openservices.loghub.client.metrics;


public class MetricReadRecord {

	private int fetchedDelay;
	private int fetchedBytes;
	private int fetchedCount;
	private int fetchedMillis;

	public int getFetchedDelay() {
		return fetchedDelay;
	}

	public void setFetchedDelay(final int fetchedDelay) {
		this.fetchedDelay = fetchedDelay;
	}

	public int getFetchedBytes() {
		return fetchedBytes;
	}

	public void setFetchedBytes(final int fetchedBytes) {
		this.fetchedBytes = fetchedBytes;
	}

	public int getFetchedCount() {
		return fetchedCount;
	}

	public void setFetchedCount(final int fetchedCount) {
		this.fetchedCount = fetchedCount;
	}

	public int getFetchedMillis() {
		return fetchedMillis;
	}

	public void setFetchedMillis(final int fetchedMillis) {
		this.fetchedMillis = fetchedMillis;
	}

}

package com.aliyun.openservices.loghub.client.metrics;


public class MetricWriteRecord {

	private int delivered;
	private int dropped;
	private int failed;
	private int processedDelay;
	private int writeCount;
	private int writeBytes;
	private int writeMillis;

	public int getDelivered() {
		return delivered;
	}

	public void setDelivered(final int delivered) {
		this.delivered = delivered;
	}

	public int getDropped() {
		return dropped;
	}

	public void setDropped(final int dropped) {
		this.dropped = dropped;
	}

	public int getFailed() {
		return failed;
	}

	public void setFailed(final int failed) {
		this.failed = failed;
	}

	public int getProcessedDelay() {
		return processedDelay;
	}

	public void setProcessedDelay(final int processedDelay) {
		this.processedDelay = processedDelay;
	}

	public int getWriteCount() {
		return writeCount;
	}

	public void setWriteCount(final int writeCount) {
		this.writeCount = writeCount;
	}

	public int getWriteBytes() {
		return writeBytes;
	}

	public void setWriteBytes(final int writeBytes) {
		this.writeBytes = writeBytes;
	}

	public int getWriteMillis() {
		return writeMillis;
	}

	public void setWriteMillis(final int writeMillis) {
		this.writeMillis = writeMillis;
	}

}

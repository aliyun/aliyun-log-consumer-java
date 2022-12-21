package com.aliyun.openservices.loghub.client.metrics.kv;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MetricValue {

	private AtomicInteger fetchedDelay = new AtomicInteger(0);
	private AtomicLong fetchedBytes = new AtomicLong(0);
	private AtomicInteger fetchedCount = new AtomicInteger(0);
	private AtomicInteger fetchedMillis = new AtomicInteger(0);

	private AtomicInteger delivered = new AtomicInteger(0);
	private AtomicInteger dropped = new AtomicInteger(0);
	private AtomicInteger failed = new AtomicInteger(0);
	private AtomicInteger processedDelay = new AtomicInteger(0);
	private AtomicInteger writeCount = new AtomicInteger(0);
	private AtomicLong writeBytes = new AtomicLong(0);
	private AtomicInteger writeMillis = new AtomicInteger(0);

	public void setFetchedDelay(int value){
		fetchedDelay.getAndSet(value);
	}

	public int getFetchedDelay(){
		return fetchedDelay.get();
	}

	public void addFetchedBytes(long value){
		fetchedBytes.addAndGet(value);
	}

	public long getFetchedBytes(){
		return fetchedBytes.get();
	}

	public void addFetchedCount(int value){
		fetchedCount.addAndGet(value);
	}

	public int getFetchedCount(){
		return fetchedCount.get();
	}

	public void addFetchedMillis(int value){
		fetchedMillis.addAndGet(value);
	}

	public int getFetchedMillis(){
		return fetchedMillis.get();
	}

	public void addDelivered(int value){
		delivered.addAndGet(value);
	}

	public int getDelivered(){
		return delivered.get();
	}

	public void addDropped(int value){
		dropped.addAndGet(value);
	}

	public int getDropped(){
		return dropped.get();
	}

	public void addFailed(int value){
		failed.addAndGet(value);
	}

	public int getFailed(){
		return failed.get();
	}

	public void setProcessedDelay(int value){
		processedDelay.getAndSet(value);
	}

	public int getProcessedDelay(){
		return processedDelay.get();
	}

	public void addWriteCount(int value){
		writeCount.addAndGet(value);
	}

	public int getWriteCount(){
		return writeCount.get();
	}

	public void addWriteBytes(long value){
		writeBytes.addAndGet(value);
	}

	public long getWriteBytes(){
		return writeBytes.get();
	}

	public void addWriteMillis(int value){
		writeMillis.addAndGet(value);
	}

	public int getWriteMillis(){
		return writeMillis.get();
	}

}

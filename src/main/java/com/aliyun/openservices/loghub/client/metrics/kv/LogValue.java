package com.aliyun.openservices.loghub.client.metrics.kv;


import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LogValue {

	private AtomicInteger count = new AtomicInteger(0);

    private AtomicReference<String> message = new AtomicReference<String>();

    public void addCount (int value){
		count.addAndGet(value);
	}

	public void incrementCount (){
		count.incrementAndGet();
	}

	public int getCount (){
		return count.get();
	}

	public String getMessage() {
		return message.get();
	}

	public void setMessage(final String message) {
		this.message.set(message);
	}
}

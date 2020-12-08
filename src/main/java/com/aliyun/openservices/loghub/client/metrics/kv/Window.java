package com.aliyun.openservices.loghub.client.metrics.kv;


public class Window {

	protected long timeIndex;
	protected long startTime;
	protected long endTime;

	public Window(){

	}

	public Window(long timeIndex, long startTime, long endTime) {
		this.timeIndex = timeIndex;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public long getTimeIndex() {
		return timeIndex;
	}

	public void setTimeIndex(final long timeIndex) {
		this.timeIndex = timeIndex;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(final long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(final long endTime) {
		this.endTime = endTime;
	}

	public void mixWindow(Window window){
		this.timeIndex = window.getTimeIndex();
		this.startTime = window.getStartTime();
		this.endTime = window.getEndTime();
	}
}

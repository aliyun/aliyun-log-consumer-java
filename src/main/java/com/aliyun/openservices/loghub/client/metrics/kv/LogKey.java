package com.aliyun.openservices.loghub.client.metrics.kv;


import com.aliyun.openservices.loghub.client.metrics.LogDimension;

public class LogKey extends Window {


	private String level;
	private String code;

	public LogKey() {

	}

	public String getLevel() {
		return level;
	}

	public String getCode() {
		return code;
	}

	public LogKey(long timeIndex, long startTime, long endTime, String level, String code) {
		this.timeIndex = timeIndex;
		this.startTime = startTime;
		this.endTime = endTime;
		this.level = level;
		this.code = code;
	}

	public void mixDimension(LogDimension logDimension) {
		this.level = logDimension.getLevel();
		this.code = logDimension.getCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		LogKey logKey = (LogKey) o;

		if (timeIndex != logKey.timeIndex) {
			return false;
		}
		if (!level.equals(logKey.level)) {
			return false;
		}
		return code.equals(logKey.code);
	}

	@Override
	public int hashCode() {
		int result = (int) (timeIndex ^ (timeIndex >>> 32));
		result = 31 * result + level.hashCode();
		result = 31 * result + code.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "LogKey{" + "timeIndex=" + timeIndex + ", startTime=" + startTime + ", endTime=" + endTime + ", level='" + level + '\'' + ", code='" + code + '\'' + '}';
	}
}

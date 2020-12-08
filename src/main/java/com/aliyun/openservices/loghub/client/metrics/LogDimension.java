package com.aliyun.openservices.loghub.client.metrics;

public class LogDimension {
	private String level;
	private String code;

	public LogDimension(String level, String code) {
		this.level = level;
		this.code = code;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final LogDimension that = (LogDimension) o;

		if (level != null ? !level.equals(that.level) : that.level != null) {
			return false;
		}
		return code != null ? code.equals(that.code) : that.code == null;
	}

	@Override
	public int hashCode() {
		int result = level != null ? level.hashCode() : 0;
		result = 31 * result + (code != null ? code.hashCode() : 0);
		return result;
	}
}

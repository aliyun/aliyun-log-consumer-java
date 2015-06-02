package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.common.LogHubModes.LogHubMode;

public class InitTaskResult extends TaskResult {

	private String mCursor;
	private LogHubMode mMode;

	public InitTaskResult(String cursor, LogHubMode mode) {
		super(null);
		mCursor = cursor;
		mMode = mode;
	}

	public String getCursor() {
		return mCursor;
	}

	public LogHubMode getMode() {
		return mMode;
	}

}

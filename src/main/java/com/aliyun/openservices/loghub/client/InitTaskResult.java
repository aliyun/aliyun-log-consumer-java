package com.aliyun.openservices.loghub.client;

public class InitTaskResult extends TaskResult {

	private String mCursor;

	public InitTaskResult(String cursor) {
		super(null);
		mCursor = cursor;
	}

	public String getCursor() {
		return mCursor;
	}

}

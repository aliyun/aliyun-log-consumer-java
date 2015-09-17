package com.aliyun.openservices.loghub.client;

public class InitTaskResult extends TaskResult {

	private String mCursor;
	private boolean mCursorPersistent = false;

	public InitTaskResult(String cursor , boolean cursor_persistent) {
		super(null);
		mCursor = cursor;
		mCursorPersistent = cursor_persistent;
	}

	public String getCursor() {
		return mCursor;
	}
	
	public boolean isCursorPersistent()
	{
		return mCursorPersistent;
	}

}

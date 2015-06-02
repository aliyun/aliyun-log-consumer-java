package com.aliyun.openservices.loghub.client.unittest;

import java.util.List;

import com.aliyun.openservices.loghub.client.LogHubClientAdapter;

class MockLogHubClientAdapter extends LogHubClientAdapter {
	private List<String> mShards;

	public MockLogHubClientAdapter() {
		super(null, null, null);
	}

	public List<String> listShard() {
		return mShards;
	}

	public void setShard(List<String> shards) {
		mShards = shards;
	}
}

package com.aliyun.openservices.loghub.client.interfaces;

/**
 * The interface used with ClientFetcher to notify external user the changes on shard list assigned on 
 * current fetcher.
 * 
 * @author guilin.xgl
 *
 */
public interface ILogHubShardListener {
	
	/**
	 * @param shardId : id for new added shard.
	 */
	void ShardAdded(int shardId);
	
	/**
	 * 
	 * @param shardId : id for deleted shard.
	 */
	void ShardDeleted(int shardId);
}

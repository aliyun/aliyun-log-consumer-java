package com.aliyun.openservices.loghub.client.interfaces;

/**
 * The interface used with ClientFetcher to notify external user the changes on shard list assigned on 
 * current fetcher.
 * Notes: ShardDeleted may be called multiple times when deleting one shard because shard shutdown is
 *  an asynchronous operation and may retry multiple times. it is external user's responsibility to process
 *  this side-effect.
 * @author guilin.xgl
 *
 */
public interface ILogHubShardListener {
	
	/**
	 * @param shardId : id for new added shard.
	 */
	void ShardAdded(String shardId);
	
	/**
	 * 
	 * @param shardId : id for deleted shard.
	 */
	void ShardDeleted(String shardId);
}

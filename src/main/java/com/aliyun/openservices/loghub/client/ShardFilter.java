package com.aliyun.openservices.loghub.client;

import java.util.List;

public interface ShardFilter {

    /**
     * Filter shards assigned to this consumer. Only the returned shards
     * will be consumed.
     *
     * @param shards All shards assigned to this consumer.
     * @return The shards to consumed.
     */
    List<Integer> filter(List<Integer> shards);
}

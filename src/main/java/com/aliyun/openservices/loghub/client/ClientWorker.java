package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.throttle.FixedResourceBarrier;
import com.aliyun.openservices.loghub.client.throttle.ResourceBarrier;
import com.aliyun.openservices.loghub.client.throttle.UnlimitedResourceBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWorker.class);

    private final ILogHubProcessorFactory processorFactory;
    private final LogHubConfig logHubConfig;
    private final LogHubHeartBeat logHubHeartBeat;
    private final Map<Integer, ShardConsumer> shardConsumer = new HashMap<Integer, ShardConsumer>();
    private final ExecutorService executorService;
    private LogHubClientAdapter loghubClient;
    private volatile boolean shutDown = false;
    private volatile boolean mainLoopExit = false;
    private ResourceBarrier resourceBarrier;
    private int lastFetchThrottleMinShard = 0;

    public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
        this(factory, config, null);
    }

    public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config, ExecutorService service) throws LogHubClientWorkerException {
        processorFactory = factory;
        logHubConfig = config;
        executorService = service == null ? Executors.newCachedThreadPool(new LogThreadFactory()) : service;
        loghubClient = new LogHubClientAdapter(config);
        loghubClient.createConsumerGroupIfNotExist(config);
        logHubHeartBeat = new LogHubHeartBeat(loghubClient, config);
        int dataSizeInMB = logHubConfig.getMaxInProgressingDataSizeInMB();
        if (dataSizeInMB > 0) {
            resourceBarrier = new FixedResourceBarrier(dataSizeInMB * 1024L * 1024L);
        } else {
            resourceBarrier = new UnlimitedResourceBarrier();
        }
    }

    public void SwitchClient(String accessKeyId, String accessKey) {
        loghubClient.SwitchClient(logHubConfig.getEndpoint(), accessKeyId, accessKey, null);
    }

    public void SwitchClient(String accessKeyId, String accessKey, String stsToken) {
        loghubClient.SwitchClient(logHubConfig.getEndpoint(), accessKeyId, accessKey, stsToken);
    }

    public void setShardFilter(ShardFilter shardFilter) {
        logHubHeartBeat.setShardFilter(shardFilter);
    }

    private static List<Integer> sortShards(List<Integer> shards) {
        if (shards == null) {
            return Collections.emptyList();
        }
        Collections.sort(shards);
        return shards;
    }

    public void run() {
        logHubHeartBeat.start();
        long fetchInterval = logHubConfig.getFetchIntervalMillis();
        while (!shutDown) {
            List<Integer> heldShards = logHubHeartBeat.getHeldShards();
            List<Integer> shards = sortShards(heldShards);
            int curFetchThrottleMinShard = -1;
            for (int shard : shards) {
                ShardConsumer consumer = consumerForShard(shard);
                if (!consumer.consume(shard >= lastFetchThrottleMinShard)) {
                    if (curFetchThrottleMinShard < 0) {
                        curFetchThrottleMinShard = shard;
                    }
                }
            }
            lastFetchThrottleMinShard = Math.max(curFetchThrottleMinShard, 0);
            cleanConsumer(heldShards);
            LoghubClientUtil.sleep(fetchInterval);
        }
        mainLoopExit = true;
    }

    public void shutdown() {
        this.shutDown = true;
        int times = 0;
        while (!mainLoopExit && times++ < 20) {
            LoghubClientUtil.sleep(1000);
        }
        for (ShardConsumer consumer : shardConsumer.values()) {
            consumer.shutdown();
        }
        LoghubClientUtil.shutdownAndAwaitTermination(executorService, 30);
        logHubHeartBeat.stop();
    }

    private void cleanConsumer(List<Integer> ownedShard) {
        List<Integer> shardToUnload = new ArrayList<Integer>();
        for (Map.Entry<Integer, ShardConsumer> entry : shardConsumer.entrySet()) {
            int shard = entry.getKey();
            if (ownedShard.contains(shard)) {
                continue;
            }
            LOG.warn("Shard {} has been assigned to another consumer.", shard);
            final ShardConsumer consumer = entry.getValue();
            if (consumer.canBeUnloaded()) {
                LOG.info("Shutting down consumer of shard: {}", shard);
                consumer.shutdown();
            } else {
                LOG.warn("Shard {} cannot be unloaded as it's checkpoint has not been committed yet", shard);
            }
            if (consumer.isShutdown()) {
                shardToUnload.add(shard);
                LOG.info("Shard shutdown done, removing from heartbeat list: {}", shard);
            }
        }
        for (Integer shard : shardToUnload) {
            shardConsumer.remove(shard);
        }
        for (Integer shard : ownedShard) {
            if (!shardConsumer.containsKey(shard)) {
                // Shard is not consuming such as filtered
                shardToUnload.add(shard);
            }
        }
        if (!shardToUnload.isEmpty()) {
            logHubHeartBeat.unsubscribe(shardToUnload);
            LOG.warn("Cancel heart beating for {}", Arrays.toString(shardToUnload.toArray()));
        }
    }

    private ShardConsumer consumerForShard(final int shardId) {
        ShardConsumer consumer = shardConsumer.get(shardId);
        if (consumer != null) {
            return consumer;
        }
        consumer = new ShardConsumer(loghubClient,
                shardId,
                processorFactory.generatorProcessor(),
                executorService,
                logHubConfig,
                logHubHeartBeat,
                resourceBarrier);
        shardConsumer.put(shardId, consumer);
        LOG.info("Create a consumer for shard: {}", shardId);
        return consumer;
    }
}

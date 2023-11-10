package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.throttle.FixedResourceBarrier;
import com.aliyun.openservices.loghub.client.throttle.ResourceBarrier;
import com.aliyun.openservices.loghub.client.throttle.UnlimitedResourceBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWorker.class);

    private final ILogHubProcessorFactory processorFactory;
    private final LogHubConfig logHubConfig;
    private final LogHubHeartBeat logHubHeartBeat;
    private final Map<Integer, ShardConsumer> shardConsumer = new HashMap<Integer, ShardConsumer>();
    private final ExecutorService executorService;
    private final LogHubClientAdapter loghubClient;
    private volatile boolean shutDown = false;
    private volatile boolean mainLoopExit = false;
    private ResourceBarrier resourceBarrier;
    private int lastFetchThrottleMinShard = 0;
    private boolean shareThreadPool;

    private ShardFilter shardFilter;
    private List<Integer> assigned;

    public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
        this(factory, config, null);
    }

    public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config, ExecutorService threadPool) throws LogHubClientWorkerException {
        processorFactory = factory;
        logHubConfig = config;
        assigned = new ArrayList<>();
        if (threadPool == null) {
            shareThreadPool = false;
            executorService = Executors.newCachedThreadPool(new LogThreadFactory());
        } else {
            shareThreadPool = true;
            executorService = threadPool;
        }
        loghubClient = new LogHubClientAdapter(config);
        try {
            loghubClient.createConsumerGroupIfNotExist(config);
        } catch (LogHubClientWorkerException ex) {
            loghubClient.shutdown();
            throw ex;
        }
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
        this.shardFilter = shardFilter;
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
            if (shardFilter != null) {
                shards = shardFilter.filter(shards);
            }
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
        if (!shareThreadPool) {
            LoghubClientUtil.shutdownThreadPool(executorService, 30);
        }
        logHubHeartBeat.stop();
        loghubClient.shutdown();
    }

    private void cleanConsumer(List<Integer> ownedShard) {
        List<Integer> shutdownComplete = new ArrayList<>();
        for (Map.Entry<Integer, ShardConsumer> entry : shardConsumer.entrySet()) {
            int shard = entry.getKey();
            final ShardConsumer consumer = entry.getValue();
            if (!ownedShard.contains(shard)) {
                LOG.warn("Shard {} has been assigned to another consumer.", shard);
                if (consumer.canBeUnloaded()) {
                    LOG.info("Shutting down consumer of shard: {}", shard);
                    consumer.shutdown();
                } else {
                    LOG.warn("Shard {} cannot be unloaded as checkpoint not updated yet", shard);
                }
            }
            if (consumer.isShutdown()) {
                shutdownComplete.add(shard);
                LOG.info("Shard shutdown done, shard={}", shard);
            }
        }
        for (int shard : assigned) {
            // Filter the shards not in consuming, such as filtered. We can remove
            // them from heart shards without unload.
            if (!shardConsumer.containsKey(shard) && !ownedShard.contains(shard)) {
                shutdownComplete.add(shard);
            }
        }
        // The following scenario is possible.
        // shard -> assigned to other consumer -> shutting down -> shutting down complete
        //                                      -> assigned back ->
        for (Integer shard : shutdownComplete) {
            shardConsumer.remove(shard);
            if (!ownedShard.contains(shard)) {
                logHubHeartBeat.removeFromHeartShards(shard);
            }
        }
        assigned = ownedShard;
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

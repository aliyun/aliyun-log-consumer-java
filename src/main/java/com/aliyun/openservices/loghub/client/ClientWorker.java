package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class ClientWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWorker.class);

    private final ILogHubProcessorFactory processorFactory;
    private final LogHubConfig logHubConfig;
    private final LogHubHeartBeat logHubHeartBeat;
    private boolean shutDown = false;
    private final Map<Integer, ShardConsumer> shardConsumer = new HashMap<Integer, ShardConsumer>();
    private final ExecutorService executorService = Executors.newCachedThreadPool(new LogThreadFactory());
    private LogHubClientAdapter loghubClient;
    private volatile boolean mainLoopExit = false;

    public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
        processorFactory = factory;
        logHubConfig = config;
        loghubClient = new LogHubClientAdapter(config);
        int timeout = (int) (config.getHeartBeatIntervalMillis() * 2 / 1000);
        try {
            loghubClient.CreateConsumerGroup(timeout, config.isConsumeInOrder());
        } catch (LogException e) {
            if ("ConsumerGroupAlreadyExist".equals(e.GetErrorCode())) {
                try {
                    loghubClient.UpdateConsumerGroup(timeout, config.isConsumeInOrder());
                } catch (LogException ex) {
                    throw new LogHubClientWorkerException("error occurs when update consumer group, errorCode: " + ex.GetErrorCode() + ", errorMessage: " + ex.GetErrorMessage());
                }
            } else {
                throw new LogHubClientWorkerException("error occurs when create consumer group, errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage());
            }
        }
        logHubHeartBeat = new LogHubHeartBeat(loghubClient, config.getHeartBeatIntervalMillis());
    }

    public void SwitchClient(String accessKeyId, String accessKey) {
        loghubClient.SwitchClient(logHubConfig.getEndpoint(), accessKeyId, accessKey, null);
    }

    public void SwitchClient(String accessKeyId, String accessKey, String stsToken) {
        loghubClient.SwitchClient(logHubConfig.getEndpoint(), accessKeyId, accessKey, stsToken);
    }

    public void run() {
        logHubHeartBeat.start();
        ArrayList<Integer> heldShards = new ArrayList<Integer>();
        while (!shutDown) {
            logHubHeartBeat.getHeldShards(heldShards);
            for (int shard : heldShards) {
                ShardConsumer consumer = getOrCreateConsumer(shard);
                consumer.consume();
            }
            cleanConsumer(heldShards);
            try {
                Thread.sleep(logHubConfig.getDataFetchIntervalMillis());
            } catch (InterruptedException e) {
            }
        }
        mainLoopExit = true;
    }

    public void shutdown() {
        this.shutDown = true;
        int times = 0;
        while (!mainLoopExit && times++ < 20) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        for (ShardConsumer consumer : shardConsumer.values()) {
            consumer.shutdown();
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        logHubHeartBeat.stop();
    }

    private void cleanConsumer(ArrayList<Integer> ownedShard) {
        ArrayList<Integer> removeShards = new ArrayList<Integer>();
        for (Entry<Integer, ShardConsumer> shard : shardConsumer.entrySet()) {
            ShardConsumer consumer = shard.getValue();
            if (!ownedShard.contains(shard.getKey())) {
                consumer.shutdown();
                LOG.info("try to shut down a consumer shard: {}", shard.getKey());
            }
            if (consumer.isShutdown()) {
                logHubHeartBeat.removeHeartShard(shard.getKey());
                removeShards.add(shard.getKey());
                LOG.info("remove a consumer shard: {}", shard.getKey());
            }
        }
        for (int shard : removeShards) {
            shardConsumer.remove(shard);
        }
    }

    private ShardConsumer getOrCreateConsumer(final int shardId) {
        ShardConsumer consumer = shardConsumer.get(shardId);
        if (consumer != null) {
            return consumer;
        }
        consumer = new ShardConsumer(loghubClient,
                shardId,
                processorFactory.generatorProcessor(),
                executorService,
                logHubConfig);
        shardConsumer.put(shardId, consumer);
        LOG.info("create a consumer shard: {}", shardId);
        return consumer;
    }
}

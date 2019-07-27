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
    private final Map<Integer, LogHubConsumer> shardConsumer = new HashMap<Integer, LogHubConsumer>();
    private final ExecutorService executorService = Executors.newCachedThreadPool(new LogThreadFactory());
    private LogHubClientAdapter logHubClientAdapter;
    private volatile boolean mainLoopExit = false;

    public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
        processorFactory = factory;
        logHubConfig = config;
        logHubClientAdapter = new LogHubClientAdapter(
                config.getEndpoint(), config.getAccessId(), config.getAccessKey(), config.getStsToken(), config.getProject(),
                config.getLogStore(), config.getConsumerGroupName(), config.getConsumerName(), config.isDirectModeEnabled());
        try {
            logHubClientAdapter.CreateConsumerGroup((int) (config.getHeartBeatIntervalMillis() * 2 / 1000), config.isConsumeInOrder());
        } catch (LogException e) {
            if ("ConsumerGroupAlreadyExist".equals(e.GetErrorCode())) {
                try {
                    logHubClientAdapter.UpdateConsumerGroup((int) (config.getHeartBeatIntervalMillis() * 2 / 1000), config.isConsumeInOrder());
                } catch (LogException e1) {
                    throw new LogHubClientWorkerException("error occurs when update consumer group, errorCode: " + e1.GetErrorCode() + ", errorMessage: " + e1.GetErrorMessage());
                }
            } else {
                throw new LogHubClientWorkerException("error occurs when create consumer group, errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage());
            }
        }
        logHubHeartBeat = new LogHubHeartBeat(logHubClientAdapter, config.getHeartBeatIntervalMillis());
    }

    public void SwitchClient(String accessKeyId, String accessKey) {
        logHubClientAdapter.SwitchClient(logHubConfig.getEndpoint(), accessKeyId, accessKey, null);
    }

    public void SwitchClient(String accessKeyId, String accessKey, String stsToken) {
        logHubClientAdapter.SwitchClient(logHubConfig.getEndpoint(), accessKeyId, accessKey, stsToken);
    }

    public void run() {
        logHubHeartBeat.start();
        ArrayList<Integer> heldShards = new ArrayList<Integer>();
        while (!shutDown) {
            logHubHeartBeat.getHeldShards(heldShards);
            for (int shard : heldShards) {
                LogHubConsumer consumer = getConsumer(shard);
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
        for (LogHubConsumer consumer : shardConsumer.values()) {
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
        for (Entry<Integer, LogHubConsumer> shard : shardConsumer.entrySet()) {
            LogHubConsumer consumer = shard.getValue();
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

    private LogHubConsumer getConsumer(final int shardId) {
        LogHubConsumer consumer = shardConsumer.get(shardId);
        if (consumer != null) {
            return consumer;
        }
        consumer = new LogHubConsumer(logHubClientAdapter, shardId,
                logHubConfig.getConsumerName(),
                processorFactory.generatorProcessor(), executorService,
                logHubConfig.getCursorPosition(),
                logHubConfig.GetCursorStartTime(),
                logHubConfig.getMaxFetchLogGroupSize());
        shardConsumer.put(shardId, consumer);
        LOG.info("create a consumer shard: {}", shardId);
        return consumer;
    }
}

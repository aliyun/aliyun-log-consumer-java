package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubShardListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClientFetcher {

    private final ILogHubProcessorFactory mLogHubProcessorFactory;
    private final LogHubConfig mLogHubConfig;
    private final LogHubHeartBeat mLogHubHeartBeat;
    private LogHubClientAdapter mLogHubClientAdapter;
    private final Map<Integer, LogHubConsumer> mShardConsumer = new HashMap<Integer, LogHubConsumer>();
    private int _curShardIndex = 0;
    private final List<Integer> mShardList = new ArrayList<Integer>();
    private final Map<Integer, FetchedLogGroup> mCachedData
            = new HashMap<Integer, FetchedLogGroup>();

    private final ExecutorService mExecutorService = Executors.newCachedThreadPool(new LogThreadFactory());
    private final ScheduledExecutorService mShardListUpdateService = Executors.newScheduledThreadPool(1);

    private final long mShardListUpdateIntervalInMills = 500L;

    private ILogHubShardListener mLogHubShardListener = null;

    public ClientFetcher(LogHubConfig config) throws LogHubClientWorkerException {
        mLogHubProcessorFactory = new InnerFetcherProcessorFactory(this);
        mLogHubConfig = config;
        mLogHubClientAdapter = new LogHubClientAdapter(config);
        try {
            mLogHubClientAdapter.CreateConsumerGroup((int) (config.getHeartBeatIntervalMillis() * 2 / 1000), config.isConsumeInOrder());
        } catch (LogException e) {
            if (e.GetErrorCode().compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0) {
                try {
                    mLogHubClientAdapter.UpdateConsumerGroup((int) (config.getHeartBeatIntervalMillis() * 2 / 1000), config.isConsumeInOrder());
                } catch (LogException e1) {
                    throw new LogHubClientWorkerException("error occour when update consumer group, errorCode: " + e1.GetErrorCode() + ", errorMessage: " + e1.GetErrorMessage());
                }
            } else {
                throw new LogHubClientWorkerException("error occour when create consumer group, errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage());
            }
        }
        mLogHubHeartBeat = new LogHubHeartBeat(mLogHubClientAdapter, config.getHeartBeatIntervalMillis());
    }

    public void SwitchClient(String accessKeyId, String accessKey) {
        mLogHubClientAdapter.SwitchClient(mLogHubConfig.getEndpoint(), accessKeyId, accessKey, null);
    }

    public void SwitchClient(String accessKeyId, String accessKey, String stsToken) {
        mLogHubClientAdapter.SwitchClient(mLogHubConfig.getEndpoint(), accessKeyId, accessKey, stsToken);
    }

    public void start() {
        mLogHubHeartBeat.start();
        mShardListUpdateService.scheduleWithFixedDelay(new ShardListUpdator(), 0L,
                mShardListUpdateIntervalInMills, TimeUnit.MILLISECONDS);

    }

    /**
     * Do not fetch any more data after calling shutdown (otherwise, the checkpoint may
     * not be saved back in time). And lease coordinator's stop give enough time to save
     * checkpoint at the same time.
     */
    public void shutdown() {
        for (LogHubConsumer consumer : mShardConsumer.values()) {
            consumer.shutdown();
        }
        mExecutorService.shutdown();
        try {
            mExecutorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        mLogHubHeartBeat.stop();
        mShardListUpdateService.shutdown();
    }

    public void registerShardListener(ILogHubShardListener listener) {
        mLogHubShardListener = listener;
    }

    public ILogHubShardListener getShardListener() {
        return mLogHubShardListener;
    }

    public FetchedLogGroup nextNoBlock() {
        FetchedLogGroup result = null;

        synchronized (mShardList) {

            for (int i = 0; i < mShardList.size(); i++) {
                // in case the number of fetcher decreased
                _curShardIndex = _curShardIndex % mShardList.size();
                int shardId = mShardList.get(_curShardIndex);
                result = mCachedData.get(shardId);
                mCachedData.put(shardId, null);

                //emit next consume on current shard no matter whether gotten data.
                LogHubConsumer consumer = mShardConsumer.get(shardId);
                if (consumer != null)
                    consumer.consume();

                _curShardIndex = (_curShardIndex + 1) % mShardList.size();
                if (result != null) {
                    break;
                }
            }
        }

        return result;
    }

    public void saveCheckPoint(int shardId, String cursor, boolean persistent)
            throws LogHubCheckPointException {

        synchronized (mShardList) {
            LogHubConsumer consumer = mShardConsumer.get(shardId);
            if (consumer != null) {
                consumer.saveCheckPoint(cursor, persistent);
            } else {
                throw new LogHubCheckPointException("Invalid shardId when saving checkpoint");
            }
        }
    }

    /*
     * update cached data from internal processor (not used externally by end users)
     */
    public void updateCachedData(int shardId, FetchedLogGroup data) {
        synchronized (mShardList) {
            mCachedData.put(shardId, data);
        }
    }

    /*
     * clean cached data from internal processor (not used externally by end users)
     */
    public void cleanCachedData(int shardId) {
        synchronized (mShardList) {
            mCachedData.remove(shardId);
        }
    }

    private class ShardListUpdator implements Runnable {
        public void run() {
            try {
                ArrayList<Integer> heldShards = new ArrayList<Integer>();
                mLogHubHeartBeat.getHeldShards(heldShards);
                for (int shard : heldShards) {
                    getConsumer(shard);
                }
                cleanConsumer(heldShards);
            } catch (Throwable t) {

            }
        }
    }

    private void cleanConsumer(ArrayList<Integer> ownedShard) {
        synchronized (mShardList) {
            ArrayList<Integer> removeShards = new ArrayList<Integer>();
            for (Entry<Integer, LogHubConsumer> shard : mShardConsumer.entrySet()) {
                LogHubConsumer consumer = shard.getValue();
                if (!ownedShard.contains(shard.getKey())) {
                    consumer.shutdown();
                }
                if (consumer.isShutdown()) {
                    mLogHubHeartBeat.removeHeartShard(shard.getKey());
                    mShardConsumer.remove(shard.getKey());
                    removeShards.add(shard.getKey());
                    mShardList.remove(shard.getKey());
                }
            }
            for (int shard : removeShards) {
                mShardConsumer.remove(shard);
            }
        }
    }

    private LogHubConsumer getConsumer(int shardId) {
        synchronized (mShardList) {
            LogHubConsumer consumer = mShardConsumer.get(shardId);
            if (consumer != null) {
                return consumer;
            }
            consumer = new LogHubConsumer(mLogHubClientAdapter,
                    shardId,
                    mLogHubProcessorFactory.generatorProcessor(),
                    mExecutorService,
                    mLogHubConfig);
            mShardConsumer.put(shardId, consumer);
            mShardList.add(shardId);
            consumer.consume();
            return consumer;
        }
    }
}
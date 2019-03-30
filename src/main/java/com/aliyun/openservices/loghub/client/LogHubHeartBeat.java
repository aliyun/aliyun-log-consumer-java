package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class LogHubHeartBeat {
    private static final Logger LOG = Logger.getLogger(LogHubHeartBeat.class);
    private static final long STOP_TIMEOUT_MILLIS = 2000L;

    private ScheduledExecutorService executorService;
    private LogHubClientAdapter mLogHubClientAdapter;
    private final long mHeartBeatIntervalMillis;
    private ArrayList<Integer> mHeldShards;
    private HashSet<Integer> mHeartShards;

    public LogHubHeartBeat(LogHubClientAdapter logHubClientAdapter,
                           long heartBeatIntervalMillis) {
        super();
        this.mLogHubClientAdapter = logHubClientAdapter;
        this.mHeartBeatIntervalMillis = heartBeatIntervalMillis;
        mHeldShards = new ArrayList<Integer>();
        mHeartShards = new HashSet<Integer>();
    }

    public void Start() {
        executorService = Executors.newScheduledThreadPool(1, new LogThreadFactory());
        executorService.scheduleWithFixedDelay(new HeartBeatRunnable(), 0L,
                mHeartBeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops background threads.
     */
    public void Stop() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                // swallow it as we're exiting
            }
        }
    }

    public synchronized void GetHeldShards(ArrayList<Integer> shards) {
        shards.clear();
        shards.addAll(mHeldShards);
    }

    public synchronized void RemoveHeartShard(final int shard) {
        mHeartShards.remove(shard);
    }

    private synchronized void HeartBeat() {
        if (mLogHubClientAdapter.HeartBeat(new ArrayList<Integer>(mHeartShards), mHeldShards)) {
            mHeartShards.addAll(mHeldShards);
        }
    }

    private class HeartBeatRunnable implements Runnable {
        @Override
        public void run() {
            try {
                HeartBeat();
            } catch (Throwable t) {
                LOG.warn("Heartbeat failed", t);
            }
        }
    }
}

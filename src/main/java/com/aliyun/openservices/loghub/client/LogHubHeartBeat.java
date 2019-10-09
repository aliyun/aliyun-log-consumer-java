package com.aliyun.openservices.loghub.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogHubHeartBeat {
    private static final Logger LOG = LoggerFactory.getLogger(LogHubHeartBeat.class);
    private static final long STOP_TIMEOUT_MILLIS = 2000L;

    private ScheduledExecutorService executorService;
    private LogHubClientAdapter client;
    private final long heartBeatIntervalMillis;
    private ArrayList<Integer> heldShards;
    private HashSet<Integer> heartShards;
    private long lastHeartBeatSuccessTime;

    public LogHubHeartBeat(LogHubClientAdapter client, long heartBeatIntervalMillis) {
        this.client = client;
        this.heartBeatIntervalMillis = heartBeatIntervalMillis;
        heldShards = new ArrayList<Integer>();
        heartShards = new HashSet<Integer>();
    }

    public void start() {
        executorService = Executors.newScheduledThreadPool(1, new LogThreadFactory());
        executorService.scheduleWithFixedDelay(new HeartBeatRunnable(), 0L,
                heartBeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops background threads.
     */
    public void stop() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(STOP_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public synchronized void getHeldShards(ArrayList<Integer> shards) {
        shards.clear();
        shards.addAll(heldShards);
    }

    public synchronized void removeHeartShard(final int shard) {
        heartShards.remove(shard);
    }

    private synchronized void heartBeat() {
        if (client.HeartBeat(new ArrayList<Integer>(heartShards), heldShards)) {
            this.lastHeartBeatSuccessTime = System.currentTimeMillis();
            heartShards.addAll(heldShards);
        } else {
            long currentTime = System.currentTimeMillis() - this.lastHeartBeatSuccessTime;
            if (currentTime > STOP_TIMEOUT_MILLIS + heartBeatIntervalMillis) {
                heldShards.clear();
                LOG.warn("Heart beat timeout, automatic reset consumer held shards");
            }
        }
    }

    private class HeartBeatRunnable implements Runnable {
        @Override
        public void run() {
            try {
                heartBeat();
            } catch (Throwable t) {
                LOG.warn("Heartbeat failed", t);
            }
        }
    }
}

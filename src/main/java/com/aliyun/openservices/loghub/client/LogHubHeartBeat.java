package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogHubHeartBeat {
    private static final Logger LOG = LoggerFactory.getLogger(LogHubHeartBeat.class);
    private static final long STOP_TIMEOUT_SECS = 10L;

    private ScheduledExecutorService executorService;
    private final LogHubClientAdapter client;
    private final long intervalMills;
    private final int timeoutSecs;
    private Set<Integer> heldShards;
    private Set<Integer> heartShards;
    private long lastSuccessTime;

    public LogHubHeartBeat(LogHubClientAdapter client,
                           LogHubConfig config) {
        this.client = client;
        this.intervalMills = config.getHeartBeatIntervalMillis();
        this.timeoutSecs = config.getTimeoutInSeconds();
        heldShards = new HashSet<Integer>();
        heartShards = new HashSet<Integer>();
    }

    public void start() {
        executorService = Executors.newScheduledThreadPool(1, new LogThreadFactory());
        executorService.scheduleWithFixedDelay(new HeartBeatRunnable(), 0L,
                intervalMills, TimeUnit.MILLISECONDS);
        LOG.info("Background heartbeat thread started, interval {}", intervalMills);
    }

    /**
     * Stops background threads.
     */
    public void stop() {
        LoghubClientUtil.shutdownThreadPool(executorService, STOP_TIMEOUT_SECS);
    }

    public synchronized List<Integer> getHeldShards() {
        return new ArrayList<>(heldShards);
    }

    public synchronized void removeFromHeartShards(int shard) {
        heartShards.remove(shard);
        LOG.warn("Cancel heart beating for shard={}", shard);
    }

    private synchronized void heartBeat() {
        long nowMillis = System.currentTimeMillis();
        try {
            LOG.debug("Sending heartbeat {}", Arrays.toString(heartShards.toArray()));
            List<Integer> shards = client.HeartBeat(new ArrayList<Integer>(heartShards));
            LOG.info("Heartbeat response: {}", shards);
            heldShards = new HashSet<Integer>(shards);
            heartShards.addAll(shards);
            lastSuccessTime = nowMillis;
        } catch (Exception ex) {
            LOG.error("Error sending heartbeat, project {}, logstore {}, consumer {}",
                    client.getProject(),
                    client.getLogstore(),
                    client.getConsumer(),
                    ex);
            if (nowMillis - lastSuccessTime > (timeoutSecs * 1000L) + intervalMills) {
                // The current consumer should already been removed from consumer group
                heldShards.clear();
                heartShards.clear();
                LOG.warn("Heartbeat failed since {}, clear held shards", lastSuccessTime);
            }
        }
    }

    public synchronized void markIdle(int shard) {
        heldShards.remove(shard);
    }

    private class HeartBeatRunnable implements Runnable {

        @Override
        public void run() {
            heartBeat();
        }
    }
}

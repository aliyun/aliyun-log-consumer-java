package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogHubHeartBeat {
    private static final Logger LOG = LoggerFactory.getLogger(LogHubHeartBeat.class);
    private static final long STOP_TIMEOUT_SECS = 10L;
    private static final int RETRY_DELAY_MS = 1000;

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
            List<Integer> shards = sendHeartbeatWithRetry();
            if (shards != null) {
                heldShards = new HashSet<Integer>(shards);
                heartShards.addAll(shards);
                lastSuccessTime = nowMillis;
            } else {
                handleHeartbeatFailure(nowMillis);
            }
        } catch (Exception ex) {
            LOG.error("Unexpected error during heartbeat, project {}, logstore {}, consumer {}",
                    client.getProject(),
                    client.getLogstore(),
                    client.getConsumer(),
                    ex);
            handleHeartbeatFailure(nowMillis);
        }
    }

    /**
     * Sends a heartbeat with one retry on network/client-side errors.
     * @return the list of held shards on success, or null on failure
     */
    private List<Integer> sendHeartbeatWithRetry() {
        List<Integer> shardsToHeart = new ArrayList<Integer>(heartShards);
        LOG.debug("Sending heartbeat {}", shardsToHeart);

        try {
            List<Integer> response = client.HeartBeat(shardsToHeart);
            LOG.info("Heartbeat succeeded, response: {}", response);
            return response;
        } catch (LogException ex) {
            if (!isRetryable(ex)) {
                LOG.error("Non-retryable heartbeat failure. errorCode={}, httpCode={}, message={}",
                        ex.getErrorCode(), ex.getHttpCode(), ex.GetErrorMessage());
                return null;
            }

            LOG.warn("Retryable heartbeat failure, attempting one retry. errorCode={}, httpCode={}, message={}",
                    ex.getErrorCode(), ex.getHttpCode(), ex.GetErrorMessage());
        }

        try {
            Thread.sleep(RETRY_DELAY_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Heartbeat retry interrupted");
            return null;
        }

        try {
            List<Integer> response = client.HeartBeat(shardsToHeart);
            LOG.info("Heartbeat retry succeeded, response: {}", response);
            return response;
        } catch (LogException retryEx) {
            LOG.error("Heartbeat retry also failed. errorCode={}, httpCode={}, message={}",
                    retryEx.getErrorCode(), retryEx.getHttpCode(), retryEx.GetErrorMessage());
            return null;
        }
    }

    private boolean isRetryable(LogException ex) {
        // Only retry for client-side/network errors (httpCode <= 0).
        // Server errors (4xx, 5xx) are not retried.
        return ex.getHttpCode() <= 0;
    }

    private void handleHeartbeatFailure(long nowMillis) {
        if (nowMillis - lastSuccessTime > (timeoutSecs * 1000L) + intervalMills) {
            heldShards.clear();
            heartShards.clear();
            LOG.warn("Heartbeat failed since {}, clear held shards", lastSuccessTime);
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

package com.aliyun.openservices.loghub.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

class LoghubClientUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LoghubClientUtil.class);

    static void shutdownThreadPool(ExecutorService pool, long timeout) {
        if (pool == null) {
            return;
        }
        pool.shutdown();
        try {
            if (!pool.awaitTermination(timeout, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(timeout, TimeUnit.SECONDS)) {
                    LOG.warn("Stopping executor pool failed");
                }
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    static void sleep(long timeToSleepMillis) {
        try {
            Thread.sleep(timeToSleepMillis);
        } catch (InterruptedException e) {
            LOG.debug("Interrupted while sleeping");
            Thread.currentThread().interrupt();
        }
    }
}

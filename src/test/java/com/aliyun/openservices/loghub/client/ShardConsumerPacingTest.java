package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.throttle.FixedResourceBarrier;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ShardConsumerPacingTest {
    @Test
    public void endOfCursorWaitDoesNotSignalWorkerToSkipOtherShards() {
        FetchExecutor executor = new FetchExecutor();
        ShardConsumer idle = newConsumer(executor, 20 * 1024 * 1024);
        assertTrue(idle.consume(true)); // initialize
        assertTrue(idle.consume(true)); // first pull
        assertTrue(idle.consume(true)); // empty response at end of cursor
        assertEquals(1, executor.fetches);
    }

    @Test
    public void onlyAnAllowedFetchDeniedMemorySignalsThrottling() {
        FetchExecutor executor = new FetchExecutor();
        ShardConsumer consumer = newConsumer(executor, 0);
        assertTrue(consumer.consume(true)); // initialize
        assertTrue(consumer.consume(false));
        assertFalse(consumer.consume(true));
        assertEquals(0, executor.fetches);
    }

    private ShardConsumer newConsumer(FetchExecutor executor, long memoryBytes) {
        LogHubConfig config = new LogHubConfig("group", "consumer", "endpoint", "project", "logs",
                null, LogHubConfig.ConsumePosition.BEGIN_CURSOR);
        return new ShardConsumer(null, 1, null, executor, config, null, new FixedResourceBarrier(memoryBytes));
    }

    private static class FetchExecutor extends ThreadPoolExecutor {
        int fetches;

        FetchExecutor() {
            super(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Future<T> submit(Callable<T> task) {
            TaskResult result;
            if (task instanceof InitializeTask) {
                result = new InitTaskResult("cursor", true);
            } else if (task instanceof LogHubFetchTask) {
                fetches++;
                result = new FetchTaskResult(Collections.emptyList(), "cursor", "cursor", 0, 0, 0, true);
            } else {
                throw new AssertionError("Unexpected task " + task);
            }
            return CompletableFuture.completedFuture((T) result);
        }
    }
}

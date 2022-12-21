package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.throttle.ResourceBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ShardConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);
    private static final long PRE_ALLOCATED_BYTES = 20 * 1024 * 1024;

    enum ConsumerStatus {
        INITIALIZING,
        PROCESSING,
        SHUTTING_DOWN,
        SHUTDOWN_COMPLETE
    }

    private int shardID;
    private LogHubClientAdapter loghubClient;
    private DefaultLogHubCheckPointTracker checkpointTracker;
    private ILogHubProcessor processor;
    private LogHubCursorPosition initialPosition;
    private int startTime;
    private int maxFetchLogGroupSize;
    private ConsumerStatus currentStatus = ConsumerStatus.INITIALIZING;
    private ITask currentTask;
    private Future<TaskResult> taskFuture;
    private Future<TaskResult> fetchDataFuture;
    private ExecutorService executorService;
    private String nextFetchCursor;
    private boolean shutdown = false;
    private FetchedLogGroup lastFetchedData;
    private long lastLogErrorTime = 0;
    private long lastFetchTime = 0;
    private int lastFetchCount = 0;
    private int lastFetchRawSize = 0;
    private int throttledCount = 0;
    private LogHubConfig config;
    private ResourceBarrier resourceBarrier;
    private long lastUnThrottledTimeInMillis = 0;

    public ShardConsumer(LogHubClientAdapter loghubClient,
                         int shardID,
                         ILogHubProcessor processor,
                         ExecutorService executorService,
                         LogHubConfig config,
                         LogHubHeartBeat heartBeat,
                         ResourceBarrier resourceBarrier) {
        this.loghubClient = loghubClient;
        this.shardID = shardID;
        this.initialPosition = config.getCursorPosition();
        this.startTime = config.GetCursorStartTime();
        this.processor = processor;
        this.checkpointTracker = new DefaultLogHubCheckPointTracker(loghubClient, config, heartBeat, shardID);
        this.executorService = executorService;
        this.maxFetchLogGroupSize = config.getMaxFetchLogGroupSize();
        this.config = config;
        this.resourceBarrier = resourceBarrier;
        this.lastUnThrottledTimeInMillis = System.currentTimeMillis();
    }

    /**
     * Description: polling shard, query status and determine whither fetch data or process data
     * Param: can create new FetchTask when allowFetch is true
     * return: return false when this shard is fetch throttled
     */
    public boolean consume(boolean fetchAllowed) {
        checkAndGenerateNextTask();
        if (this.currentStatus.equals(ConsumerStatus.PROCESSING) && lastFetchedData == null) {
            if (!fetchData(fetchAllowed)) {
                return !fetchAllowed;
            }
        }
        return true;
    }

    public void saveCheckPoint(String cursor, boolean persistent)
            throws LogHubCheckPointException {
        checkpointTracker.saveCheckPoint(cursor, persistent);
    }

    private void checkAndGenerateNextTask() {
        if (taskFuture == null || taskFuture.isCancelled() || taskFuture.isDone()) {
            boolean taskSuccess = false;
            TaskResult result = getTaskResult(taskFuture, false);
            taskFuture = null;
            if (result != null && result.getException() == null) {
                taskSuccess = true;
                if (currentStatus.equals(ConsumerStatus.INITIALIZING)) {
                    InitTaskResult initResult = (InitTaskResult) (result);
                    nextFetchCursor = initResult.getCursor();
                    checkpointTracker.setInitialCursor(nextFetchCursor);
                    if (initResult.isCursorPersistent()) {
                        checkpointTracker.setInPersistentCheckPoint(nextFetchCursor);
                    }
                } else if (result instanceof ProcessTaskResult) {
                    ProcessTaskResult processTaskResult = (ProcessTaskResult) (result);
                    String checkpoint = processTaskResult.getRollBackCheckpoint();
                    if (checkpoint != null && !checkpoint.isEmpty()) {
                        cancelCurrentFetch();
                        nextFetchCursor = checkpoint;
                    }
                }
            }
            sampleLogError(result);
            updateStatus(taskSuccess);
            generateNextTask();
        }
    }

    private boolean checkThrottled() {
        long nowInMillis = System.currentTimeMillis();
        if (resourceBarrier.tryAcquire(PRE_ALLOCATED_BYTES)) {
            this.lastUnThrottledTimeInMillis = nowInMillis;
            return false;
        }
        throttledCount++;
        if (throttledCount % 200 == 0) {
            throttledCount = 0;
            if (nowInMillis - this.lastUnThrottledTimeInMillis > 900 * 1000) {
                LOG.error("ShardConsumeThrottledWarning, Fetch request throttled more than 900 seconds, shard {}", shardID);
            } else {
                LOG.warn("Fetch request throttled, shard {}", shardID);
            }
        }
        return true;
    }

    private boolean shouldFetchNext(boolean hasError) {
        if (hasError) {
            return false;
        }
        long currentNow = System.currentTimeMillis();
        boolean allowFetch;
        if (lastFetchRawSize < 1024 * 1024 && lastFetchCount < 100 && lastFetchCount < maxFetchLogGroupSize) {
            allowFetch = currentNow - lastFetchTime > 500;
        } else if (lastFetchRawSize < 2 * 1024 * 1024 && lastFetchCount < 500 && lastFetchCount < maxFetchLogGroupSize) {
            allowFetch = currentNow - lastFetchTime > 200;
        } else if (lastFetchRawSize < 4 * 1024 * 1024 && lastFetchCount < 1000 && lastFetchCount < maxFetchLogGroupSize) {
            allowFetch = currentNow - lastFetchTime > 50;
        } else {
            allowFetch = true;
        }
        if (!allowFetch) {
            return false;
        }
        //checkThrottled will acquire resource from barrier; do not move position of this line
        return !checkThrottled();
    }

    private boolean fetchData(boolean fetchAllowed) {
        boolean hasError = false;
        if (fetchDataFuture != null) {
            if (fetchDataFuture.isCancelled()) {
                fetchDataFuture = null;
                lastFetchedData = null;
                resourceBarrier.release(PRE_ALLOCATED_BYTES);
                return true;
            } else if (!fetchDataFuture.isDone()) {
                return true;
            }
            TaskResult result = getTaskResult(fetchDataFuture, false);
            if (result != null && result.getException() == null) {
                FetchTaskResult fetchResult = (FetchTaskResult) result;
                List<LogGroupData> fetchedData = fetchResult.getFetchedData();
                lastFetchedData = new FetchedLogGroup(
                        shardID,
                        fetchedData,
                        fetchResult.getNextCursor(),
                        fetchResult.getCursor());
                nextFetchCursor = fetchResult.getNextCursor();
                lastFetchCount = fetchedData.size();
                lastFetchRawSize = fetchResult.getRawSize();
                resourceBarrier.acquire(lastFetchRawSize - PRE_ALLOCATED_BYTES);
                sampleLogError(result);
                hasError = result.getException() != null;
            } else {
                resourceBarrier.release(PRE_ALLOCATED_BYTES);
            }
        }
        if (fetchAllowed && shouldFetchNext(hasError)) {
            lastFetchTime = System.currentTimeMillis();
            LogHubFetchTask task = new LogHubFetchTask(loghubClient, shardID, nextFetchCursor, config);
            fetchDataFuture = executorService.submit(task);
        } else {
            fetchDataFuture = null;
            return false;
        }
        return true;
    }

    private void sampleLogError(TaskResult result) {
        if (result != null && result.getException() != null) {
            long curTime = System.currentTimeMillis();
            if (curTime - lastLogErrorTime > 5 * 1000) {
                LOG.warn("", result.getException());
                lastLogErrorTime = curTime;
            }
        }
    }

    private TaskResult getTaskResult(Future<TaskResult> future, boolean canceled) {
        if (future != null && (future.isDone() || future.isCancelled())) {
            try {
                return future.get();
            } catch (CancellationException ex) {
                if (!canceled) {
                    LOG.warn("Task was been unexpected canceled");
                }
            } catch (final Exception ex) {
                LOG.error("Error retrieving task result", ex);
            }
        }
        return null;
    }

    private void cancelCurrentFetch() {
        if (fetchDataFuture != null) {
            fetchDataFuture.cancel(true);
            getTaskResult(fetchDataFuture, true);
            fetchDataFuture = null;
            resourceBarrier.release(PRE_ALLOCATED_BYTES);
            LOG.info("Cancel a fetch task, shard id: {}", shardID);
        }
        lastFetchedData = null;
    }

    private void generateNextTask() {
        ITask nextTask = null;
        if (this.currentStatus.equals(ConsumerStatus.INITIALIZING)) {
            nextTask = new InitializeTask(processor, loghubClient, shardID, initialPosition, startTime);
        } else if (this.currentStatus.equals(ConsumerStatus.PROCESSING)) {
            if (lastFetchedData != null) {
                checkpointTracker.setCurrentCursor(lastFetchedData.getCursor());
                checkpointTracker.setNextCursor(lastFetchedData.getNextCursor());
                nextTask = new ProcessTask(processor,
                        lastFetchedData.getFetchedData(),
                        checkpointTracker,
                        lastFetchRawSize,
                        resourceBarrier);
                lastFetchedData = null;
            }
        } else if (this.currentStatus.equals(ConsumerStatus.SHUTTING_DOWN)) {
            if (lastFetchedData != null) {
                resourceBarrier.release(lastFetchRawSize);
            }
            nextTask = new ShutDownTask(processor, checkpointTracker);
            cancelCurrentFetch();
        }
        if (nextTask != null) {
            currentTask = nextTask;
            taskFuture = executorService.submit(currentTask);
        }
    }

    private void updateStatus(boolean taskSuccess) {
        if (currentStatus.equals(ConsumerStatus.SHUTTING_DOWN)) {
            if (currentTask == null || taskSuccess) {
                currentStatus = ConsumerStatus.SHUTDOWN_COMPLETE;
            }
        } else if (shutdown) {
            currentStatus = ConsumerStatus.SHUTTING_DOWN;
        } else if (taskSuccess) {
            if (currentStatus.equals(ConsumerStatus.INITIALIZING)) {
                currentStatus = ConsumerStatus.PROCESSING;
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
        if (!isShutdown()) {
            checkAndGenerateNextTask();
        }
    }

    public boolean isShutdown() {
        return currentStatus.equals(ConsumerStatus.SHUTDOWN_COMPLETE);
    }

    /**
     * Determines if the current shard can be uploaded. Return false only if
     * {@code unloadAfterCommitEnabled} is true and the latest cursor has not
     * been committed.
     */
    boolean canBeUnloaded() {
        if (!config.isUnloadAfterCommitEnabled()) {
            return true;
        }
        // check all cursor has been committed
        return checkpointTracker.isAllCommitted();
    }
}

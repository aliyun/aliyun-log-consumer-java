package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class LogHubConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(LogHubConsumer.class);

    enum ConsumerStatus {
        INITIALIZING, PROCESSING, SHUTTING_DOWN, SHUTDOWN_COMPLETE
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

    public LogHubConsumer(LogHubClientAdapter loghubClient,
                          int shardID,
                          ILogHubProcessor processor,
                          ExecutorService executorService,
                          LogHubConfig config) {
        this.loghubClient = loghubClient;
        this.shardID = shardID;
        this.initialPosition = config.getCursorPosition();
        this.startTime = config.GetCursorStartTime();
        this.processor = processor;
        this.checkpointTracker = new DefaultLogHubCheckPointTracker(loghubClient, config, shardID);
        this.executorService = executorService;
        this.maxFetchLogGroupSize = config.getMaxFetchLogGroupSize();
    }

    public void consume() {
        checkAndGenerateNextTask();
        if (this.currentStatus.equals(ConsumerStatus.PROCESSING)
                && lastFetchedData == null) {
            fetchData();
        }
    }

    public void saveCheckPoint(String cursor, boolean persistent)
            throws LogHubCheckPointException {
        checkpointTracker.saveCheckPoint(cursor, persistent);
    }

    private void checkAndGenerateNextTask() {
        if (taskFuture == null || taskFuture.isCancelled()
                || taskFuture.isDone()) {
            boolean taskSuccess = false;
            TaskResult result = getTaskResult(taskFuture);
            taskFuture = null;
            if (result != null && result.getException() == null) {
                taskSuccess = true;
                if (currentStatus.equals(ConsumerStatus.INITIALIZING)) {
                    InitTaskResult initResult = (InitTaskResult) (result);
                    nextFetchCursor = initResult.getCursor();
                    checkpointTracker.setInMemoryCheckPoint(nextFetchCursor);
                    if (initResult.isCursorPersistent()) {
                        checkpointTracker.setInPersistentCheckPoint(nextFetchCursor);
                    }
                } else if (result instanceof ProcessTaskResult) {
                    ProcessTaskResult processTaskResult = (ProcessTaskResult) (result);
                    String checkpoint = processTaskResult.getRollBackCheckpoint();
                    if (checkpoint != null && !checkpoint.isEmpty()) {
                        lastFetchedData = null;
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

    private void fetchData() {
        if (fetchDataFuture == null || fetchDataFuture.isCancelled()
                || fetchDataFuture.isDone()) {
            TaskResult result = getTaskResult(fetchDataFuture);
            if (result != null && result.getException() == null) {
                FetchTaskResult fetchResult = (FetchTaskResult) result;
                lastFetchedData = new FetchedLogGroup(shardID,
                        fetchResult.getFetchedData(), fetchResult.getCursor());
                nextFetchCursor = fetchResult.getCursor();
                lastFetchCount = lastFetchedData.getFetchedData().size();
                lastFetchRawSize = fetchResult.getRawSize();
            }

            sampleLogError(result);

            if (result == null || result.getException() == null) {
                boolean genFetchTask = true;
                if (lastFetchRawSize < 1024 * 1024 && lastFetchCount < 100 && lastFetchCount < maxFetchLogGroupSize) {
                    genFetchTask = (System.currentTimeMillis() - lastFetchTime > 500);
                } else if (lastFetchRawSize < 2 * 1024 * 1024 && lastFetchCount < 500 && lastFetchCount < maxFetchLogGroupSize) {
                    genFetchTask = (System.currentTimeMillis() - lastFetchTime > 200);
                } else if (lastFetchRawSize < 4 * 1024 * 1024 && lastFetchCount < 1000 && lastFetchCount < maxFetchLogGroupSize) {
                    genFetchTask = (System.currentTimeMillis() - lastFetchTime > 50);
                }
                if (genFetchTask) {
                    lastFetchTime = System.currentTimeMillis();
                    LogHubFetchTask task = new LogHubFetchTask(loghubClient, shardID, nextFetchCursor, maxFetchLogGroupSize);
                    fetchDataFuture = executorService.submit(task);
                } else {
                    fetchDataFuture = null;
                }
            } else {
                fetchDataFuture = null;
            }

        }
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

    private TaskResult getTaskResult(Future<TaskResult> future) {
        if (future != null && (future.isDone() || future.isCancelled())) {
            try {
                return future.get();
            } catch (Exception e) {
                LOG.error("Error while executing task", e.getCause());
            }
        }
        return null;
    }

    private void cancelCurrentFetch() {
        if (fetchDataFuture != null) {
            fetchDataFuture.cancel(true);
            getTaskResult(fetchDataFuture);
            LOG.info("Cancel a fetch task, shard id: {}", shardID);
            fetchDataFuture = null;
        }
    }

    private void generateNextTask() {
        ITask nextTask = null;
        if (this.currentStatus.equals(ConsumerStatus.INITIALIZING)) {
            nextTask = new InitializeTask(processor, loghubClient, shardID, initialPosition, startTime);
        } else if (this.currentStatus.equals(ConsumerStatus.PROCESSING)) {
            if (lastFetchedData != null) {
                checkpointTracker.setCursor(lastFetchedData.getEndCursor());
                nextTask = new ProcessTask(processor,
                        lastFetchedData.getFetchedData(), checkpointTracker);
                lastFetchedData = null;
            }
        } else if (this.currentStatus.equals(ConsumerStatus.SHUTTING_DOWN)) {
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

}

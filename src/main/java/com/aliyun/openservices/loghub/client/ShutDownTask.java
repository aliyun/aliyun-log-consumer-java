package com.aliyun.openservices.loghub.client;


import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutDownTask implements ITask {
    private static final Logger LOG = LoggerFactory.getLogger(ShutDownTask.class);

    private ILogHubProcessor processor;
    private DefaultLogHubCheckPointTracker checkPointTracker;

    public ShutDownTask(ILogHubProcessor processor,
                        DefaultLogHubCheckPointTracker checkPointTracker) {
        this.processor = processor;
        this.checkPointTracker = checkPointTracker;
    }

    public TaskResult call() {
        Exception exception = null;
        try {
            processor.shutdown(checkPointTracker);
        } catch (Exception ex) {
            exception = ex;
            LOG.error("Could not shutdown processor", ex);
        }
        try {
            checkPointTracker.flushCheckpoint();
        } catch (Exception ex) {
            LOG.error("Failed to flush checkpoint", ex);
            if (exception == null) {
                exception = ex;
            }
        }
        return new TaskResult(exception);
    }

}

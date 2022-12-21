package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.throttle.ResourceBarrier;

public class ProcessTask implements ITask {

    private ILogHubProcessor processor;
    private List<LogGroupData> chunk;
    private DefaultLogHubCheckPointTracker checkPointTracker;
    private int rawSize;
    private ResourceBarrier resourceBarrier;

    public ProcessTask(ILogHubProcessor processor, List<LogGroupData> logGroups, DefaultLogHubCheckPointTracker checkPointTracker,
                       int rawSize, ResourceBarrier resourceBarrier) {
        this.processor = processor;
        this.chunk = logGroups;
        this.checkPointTracker = checkPointTracker;
        this.rawSize = rawSize;
        this.resourceBarrier = resourceBarrier;
    }

    public TaskResult call() {
        String checkpoint;
        try {
            checkpoint = processor.process(chunk, checkPointTracker);
            checkPointTracker.flushCheckpointIfNeeded();
        } catch (Exception e) {
            resourceBarrier.release(rawSize);
            return new TaskResult(e);
        }
        resourceBarrier.release(rawSize);
        return new ProcessTaskResult(checkpoint);
    }
}

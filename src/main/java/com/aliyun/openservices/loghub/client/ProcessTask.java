package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.throttle.ResourceBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessTask implements ITask {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessTask.class);

    private ILogHubProcessor processor;
    private List<LogGroupData> chunk;
    private DefaultLogHubCheckPointTracker checkPointTracker;
    private long rawSize;
    private ResourceBarrier resourceBarrier;

    private int shard;

    public ProcessTask(ILogHubProcessor processor,
                       List<LogGroupData> logGroups,
                       DefaultLogHubCheckPointTracker checkPointTracker,
                       long rawSize,
                       ResourceBarrier resourceBarrier,
                       int shard) {
        this.processor = processor;
        this.chunk = logGroups;
        this.checkPointTracker = checkPointTracker;
        this.rawSize = rawSize;
        this.resourceBarrier = resourceBarrier;
        this.shard = shard;
    }

    public TaskResult call() {
        String checkpoint;
        try {
            checkpoint = processor.process(chunk, checkPointTracker);
            checkPointTracker.flushCheckpointIfNeeded();
        } catch (Exception e) {
            LOG.error("Process exception occurred, shard={}", shard, e);
            resourceBarrier.release(rawSize);
            return new TaskResult(e);
        }
        resourceBarrier.release(rawSize);
        return new ProcessTaskResult(checkpoint);
    }
}

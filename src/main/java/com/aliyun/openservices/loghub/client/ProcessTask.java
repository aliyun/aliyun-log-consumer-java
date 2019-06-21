package com.aliyun.openservices.loghub.client;

import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;

public class ProcessTask implements ITask {

    private ILogHubProcessor processor;
    private List<LogGroupData> chunk;
    private DefaultLogHubCheckPointTracker checkPointTracker;

    public ProcessTask(ILogHubProcessor processor, List<LogGroupData> logGroups,
                       DefaultLogHubCheckPointTracker checkPointTracker) {
        this.processor = processor;
        this.chunk = logGroups;
        this.checkPointTracker = checkPointTracker;
    }

    public TaskResult call() {
        String checkpoint;
        try {
            checkpoint = processor.process(chunk, checkPointTracker);
            checkPointTracker.flushCheck();
        } catch (Exception e) {
            return new TaskResult(e);
        }
        return new ProcessTaskResult(checkpoint);
    }
}

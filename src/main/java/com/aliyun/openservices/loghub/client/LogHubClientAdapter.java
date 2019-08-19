package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LogHubClientAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(LogHubClientAdapter.class);

    private Client client;
    private ReadWriteLock readWrtlock = new ReentrantReadWriteLock();
    private final String project;
    private final String logstore;
    private final String consumerGroup;
    private final String consumer;
    private final boolean useDirectMode;

    private static final String DEFAULT_USER_AGENT = "loghub-client-library-java-0.6.17";

    LogHubClientAdapter(final LogHubConfig config) {
        this.useDirectMode = config.isDirectModeEnabled();
        this.client = new Client(config.getEndpoint(), config.getAccessId(), config.getAccessKey());
        if (this.useDirectMode) {
            this.client.EnableDirectMode();
        }
        if (config.getStsToken() != null) {
            this.client.setSecurityToken(config.getStsToken());
        }
        this.project = config.getProject();
        this.logstore = config.getLogStore();
        this.consumerGroup = config.getConsumerGroupName();
        this.consumer = config.getConsumerName();
        if (config.getUserAgent() != null) {
            client.setUserAgent(config.getUserAgent());
        } else {
            client.setUserAgent(DEFAULT_USER_AGENT);
        }
    }

    public void SwitchClient(String endPoint, String accessKeyId, String accessKey, String stsToken) {
        readWrtlock.writeLock().lock();
        this.client = new Client(endPoint, accessKeyId, accessKey);
        if (this.useDirectMode) {
            this.client.EnableDirectMode();
        }
        if (stsToken != null) {
            this.client.setSecurityToken(stsToken);
        }
        readWrtlock.writeLock().unlock();
    }

    public void CreateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException {
        readWrtlock.readLock().lock();
        try {
            client.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroup, timeoutInSec, inOrder));
        } finally {
            readWrtlock.readLock().unlock();
        }
    }

    public void UpdateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException {
        readWrtlock.readLock().lock();
        try {
            client.UpdateConsumerGroup(project, logstore, consumerGroup, inOrder, timeoutInSec);
        } finally {
            readWrtlock.readLock().unlock();
        }
    }

    public boolean HeartBeat(ArrayList<Integer> shards, ArrayList<Integer> response) {
        readWrtlock.readLock().lock();
        response.clear();
        try {
            response.addAll(client.HeartBeat(project, logstore, consumerGroup, consumer, shards).GetShards());
            return true;
        } catch (LogException e) {
            LOG.warn("Error while sending heartbeat", e);
        } finally {
            readWrtlock.readLock().unlock();
        }
        return false;
    }

    public void UpdateCheckPoint(final int shard, final String consumer, final String checkpoint) throws LogException {
        readWrtlock.readLock().lock();
        try {
            client.UpdateCheckPoint(project, logstore, consumerGroup, consumer, shard, checkpoint);
        } finally {
            readWrtlock.readLock().unlock();
        }
    }

    public String GetCheckPoint(final int shard) throws LogException {
        readWrtlock.readLock().lock();
        ConsumerGroupCheckPointResponse response;
        try {
            response = client.GetCheckPoint(project, logstore, consumerGroup, shard);
        } finally {
            readWrtlock.readLock().unlock();
        }
        // TODO move this to SDK
        List<ConsumerGroupShardCheckPoint> checkpoints = response.getCheckPoints();
        if (checkpoints == null || checkpoints.isEmpty()) {
            throw new LogException("CheckpointNotExist", "Checkpoint not found for shard " + shard, response.GetRequestId());
        }
        return checkpoints.get(0).getCheckPoint();
    }

    public String GetCursor(final int shard, CursorMode mode) throws LogException {
        readWrtlock.readLock().lock();
        try {
            return client.GetCursor(project, logstore, shard, mode).GetCursor();
        } finally {
            readWrtlock.readLock().unlock();
        }
    }

    public String GetCursor(final int shard, final long time) throws LogException {
        readWrtlock.readLock().lock();
        try {
            return client.GetCursor(project, logstore, shard, time).GetCursor();
        } finally {
            readWrtlock.readLock().unlock();
        }
    }

    public BatchGetLogResponse BatchGetLogs(final int shard, final int lines, final String cursor) throws LogException {
        readWrtlock.readLock().lock();
        try {
            return client.BatchGetLog(project, logstore, shard, lines, cursor);
        } finally {
            readWrtlock.readLock().unlock();
        }
    }
}
package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;

public class LogHubClientAdapter {

    private Client client;
    private ReadWriteLock readWrtlock = new ReentrantReadWriteLock();
    private final String project;
    private final String logstore;
    private final String consumerGroup;
    private final String consumer;
    private final boolean useDirectMode;
    private static final Logger logger = Logger.getLogger(LogHubClientAdapter.class);

    public LogHubClientAdapter(String endPoint, String accessKeyId, String accessKey, String stsToken, String project, String logstore,
                               String consumerGroup, String consumer, boolean useDirectMode) {
        this.useDirectMode = useDirectMode;
        this.client = new Client(endPoint, accessKeyId, accessKey);
        if (this.useDirectMode) {
            this.client.EnableDirectMode();
        }
        if (stsToken != null) {
            this.client.SetSecurityToken(stsToken);
        }
        this.project = project;
        this.logstore = logstore;
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
        this.client.setUserAgent("consumergroup-java-" + consumerGroup);
    }

    public void SwitchClient(String endPoint, String accessKeyId, String accessKey, String stsToken) {
        readWrtlock.writeLock().lock();
        this.client = new Client(endPoint, accessKeyId, accessKey);
        if (this.useDirectMode) {
            this.client.EnableDirectMode();
        }
        if (stsToken != null) {
            this.client.SetSecurityToken(stsToken);
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
            logger.warn(e.GetErrorCode() + ": " + e.GetErrorMessage());
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

    public String GetCheckPoint(final int shard) throws LogException, LogHubCheckPointException {
        readWrtlock.readLock().lock();
        ArrayList<ConsumerGroupShardCheckPoint> checkPoints = null;
        try {
            checkPoints = client.GetCheckPoint(project, logstore, consumerGroup, shard).GetCheckPoints();
        } finally {
            readWrtlock.readLock().unlock();
        }
        if (checkPoints == null || checkPoints.size() == 0) {
            throw new LogHubCheckPointException("fail to get shard checkpoint");
        } else {
            return checkPoints.get(0).getCheckPoint();
        }
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
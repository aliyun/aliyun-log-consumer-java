package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LogHubClientAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(LogHubClientAdapter.class);

    private Client client;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final String project;
    private final String logstore;
    private final String consumerGroupName;
    private final String consumer;
    private final String userAgent;
    private final LogHubConfig config;

    LogHubClientAdapter(final LogHubConfig config) {
        this.config = config;
        this.project = config.getProject();
        this.logstore = config.getLogStore();
        this.consumerGroupName = config.getConsumerGroup();
        this.consumer = config.getConsumer();
        this.userAgent = getOrCreateUserAgent(config);
        this.client = createClient(config);
    }

    private static ClientConfiguration getClientConfiguration(LogHubConfig config) {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setMaxConnections(Consts.HTTP_CONNECT_MAX_COUNT);
        clientConfig.setConnectionTimeout(Consts.HTTP_CONNECT_TIME_OUT);
        clientConfig.setSocketTimeout(Consts.HTTP_SEND_TIME_OUT);
        clientConfig.setUseReaper(true);
        clientConfig.setProxyHost(config.getProxyHost());
        clientConfig.setProxyPort(config.getProxyPort());
        clientConfig.setProxyUsername(config.getProxyUsername());
        clientConfig.setProxyPassword(config.getProxyPassword());
        clientConfig.setProxyDomain(config.getProxyDomain());
        clientConfig.setProxyWorkstation(config.getProxyWorkstation());
        clientConfig.setSignatureVersion(config.getSignVersion());
        clientConfig.setRegion(config.getRegion());
        return clientConfig;
    }

    private Client createClient(String endpoint, String accessKeyId, String accessKey, String stsToken) {
        ClientConfiguration clientConfig = getClientConfiguration(config);
        Client client = new Client(endpoint, accessKeyId, accessKey, clientConfig);
        if (stsToken != null) {
            client.setSecurityToken(stsToken);
        }
        client.setUserAgent(userAgent);
        client.setUseDirectMode(config.isDirectModeEnabled());
        return client;
    }

    private Client createClient(LogHubConfig config) {
        if (config.getCredentialsProvider() == null) {
            return createClient(config.getEndpoint(), config.getAccessId(), config.getAccessKey(), config.getStsToken());
        }

        ClientConfiguration clientConfig = getClientConfiguration(config);
        Client client = new Client(config.getEndpoint(), config.getCredentialsProvider(), clientConfig, null);
        client.setUserAgent(userAgent);
        client.setUseDirectMode(config.isDirectModeEnabled());
        return client;
    }

    public void shutdown() {
        freeClient();
    }

    private void freeClient() {
        if (client != null) {
            // Free resource in HTTP service client.
            client.shutdown();
        }
    }

    public String getProject() {
        return project;
    }

    public String getLogstore() {
        return logstore;
    }

    public String getConsumer() {
        return consumer;
    }

    private static String getOrCreateUserAgent(LogHubConfig config) {
        if (config.getUserAgent() != null) {
            return config.getUserAgent();
        }
        return "Consumer-Library-" + config.getConsumerGroup() + "/" + config.getConsumer();
    }

    public void SwitchClient(String endpoint, String accessKeyId, String accessKey, String stsToken) {
        lock.writeLock().lock();
        freeClient();
        this.client = createClient(endpoint, accessKeyId, accessKey, stsToken);
        lock.writeLock().unlock();
    }

    private ConsumerGroup getConsumerGroup(String consumerGroupName) throws LogException {
        ListConsumerGroupResponse response = client.ListConsumerGroup(project, logstore);
        if (response != null) {
            for (ConsumerGroup item : response.GetConsumerGroups()) {
                if (item.getConsumerGroupName().equalsIgnoreCase(consumerGroupName)) {
                    return item;
                }
            }
        }
        return null;
    }

    void createConsumerGroupIfNotExist(LogHubConfig config) throws LogHubClientWorkerException {
        LOG.info("Start client worker: {}", config.toString());
        lock.readLock().lock();
        try {
            boolean exist = false;
            try {
                ConsumerGroup consumerGroup = getConsumerGroup(consumerGroupName);
                if (consumerGroup != null) {
                    if (consumerGroup.getTimeout() == config.getTimeoutInSeconds()
                            && consumerGroup.isInOrder() == config.isConsumeInOrder()) {
                        LOG.info("Consumer Group {} already exist", consumerGroupName);
                        return;
                    }
                    exist = true;
                }
            } catch (LogException ex) {
                LOG.warn("Error checking consumer group", ex);
                // do not throw exception here for bwc
            }
            if (!exist) {
                try {
                    client.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroupName,
                            config.getTimeoutInSeconds(),
                            config.isConsumeInOrder()));
                    LOG.info("Create ConsumerGroup {} success.", consumerGroupName);
                    return;
                } catch (LogException ex) {
                    if (!"ConsumerGroupAlreadyExist".equalsIgnoreCase(ex.GetErrorCode())) {
                        throw new LogHubClientWorkerException("error occurs when create consumer group, errorCode: "
                                + ex.GetErrorCode()
                                + ", errorMessage: "
                                + ex.GetErrorMessage(),
                                ex);
                    }
                }
            }
            try {
                UpdateConsumerGroup(config.getTimeoutInSeconds(), config.isConsumeInOrder());
                LOG.info("Update ConsumerGroup {} success.", consumerGroupName);
            } catch (LogException ex2) {
                throw new LogHubClientWorkerException("error occurs when update consumer group, errorCode: "
                        + ex2.GetErrorCode()
                        + ", errorMessage: "
                        + ex2.GetErrorMessage(),
                        ex2);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void CreateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException {
        lock.readLock().lock();
        try {
            client.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroupName, timeoutInSec, inOrder));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void UpdateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException {
        lock.readLock().lock();
        try {
            client.UpdateConsumerGroup(project, logstore, consumerGroupName, inOrder, timeoutInSec);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Integer> HeartBeat(ArrayList<Integer> shards) throws LogException {
        lock.readLock().lock();
        try {
            return client.HeartBeat(project, logstore, consumerGroupName, consumer, shards).getShards();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void UpdateCheckPoint(final int shard, final String consumer, final String checkpoint) throws LogException {
        lock.readLock().lock();
        try {
            client.UpdateCheckPoint(project, logstore, consumerGroupName, consumer, shard, checkpoint);
        } finally {
            lock.readLock().unlock();
        }
    }

    public String GetCheckPoint(final int shard) throws LogException {
        lock.readLock().lock();
        ConsumerGroupCheckPointResponse response;
        try {
            response = client.GetCheckPoint(project, logstore, consumerGroupName, shard);
        } finally {
            lock.readLock().unlock();
        }
        // TODO move this to SDK
        List<ConsumerGroupShardCheckPoint> checkpoints = response.getCheckPoints();
        if (checkpoints == null || checkpoints.isEmpty()) {
            throw new LogException("CheckpointNotExist", "Checkpoint not found for shard " + shard, response.GetRequestId());
        }
        return checkpoints.get(0).getCheckPoint();
    }

    public String GetCursor(final int shard, CursorMode mode) throws LogException {
        lock.readLock().lock();
        try {
            return client.GetCursor(project, logstore, shard, mode).GetCursor();
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getCursor(int shard, LogHubCursorPosition position, long startTime) throws LogException {
        if (position.equals(LogHubCursorPosition.BEGIN_CURSOR)) {
            return GetCursor(shard, CursorMode.BEGIN);
        } else if (position.equals(LogHubCursorPosition.END_CURSOR)) {
            return GetCursor(shard, CursorMode.END);
        } else {
            return GetCursor(shard, startTime);
        }
    }

    public String GetCursor(final int shard, final long time) throws LogException {
        lock.readLock().lock();
        try {
            return client.GetCursor(project, logstore, shard, time).GetCursor();
        } finally {
            lock.readLock().unlock();
        }
    }

    public BatchGetLogResponse BatchGetLogs(final int shard, final int lines, final String cursor) throws LogException {
        lock.readLock().lock();
        try {
            return client.BatchGetLog(project, logstore, shard, lines, cursor);
        } finally {
            lock.readLock().unlock();
        }
    }
}
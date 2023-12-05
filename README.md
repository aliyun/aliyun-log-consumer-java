# Consumer Library User Guide

English | [中文](./README-zh_CN.md)

The Aliyun LOG Consumer Library is a Java library for consuming data from Logstore that simplifies the data consumption process by automatically managing load balancing, checkpoint saving, sequential consumption, and exception handling.


## Features

- **Easy to use**: Simply configure and implement your data processing logic without worrying about load balancing, checkpointing, or exception handling.
- **High performance**: Enhances throughput and efficiency with multi-threaded asynchronous data fetching and processing.
- **Load balancing**: Automatically balances the load based on the number of consumers in the ConsumerGroup and the number of Shards.
- **Automatic retry**: Automatically retries transient exceptions that occur during execution, without causing data duplication.
- **Thread-safe**: All exposed methods and interfaces are thread-safe.
- **Graceful shutdown**: Waits for the completion of asynchronous tasks and commits the current consumption checkpoint to the server when the shutdown interface is called.


## How To Use

Using the Consumer Library primarily involves three steps:

1. Add dependencies;
2. Implement two interfaces from the Consumer Library to write business logic:
    - `ILogHubProcessor`: Each Shard corresponds to an instance, with each instance consuming data from a specific Shard only;
    - `ILogHubProcessorFactory`: A factory object responsible for creating instances that implement the ILogHubProcessor;
3. Start one or more ClientWorker instances.

### Add Dependencies

Using Maven as an example, add the following dependencies in `pom.xml`:

```xml
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>2.5.0</version>
</dependency>
<dependency>
<groupId>com.aliyun.openservices</groupId>
<artifactId>loghub-client-lib</artifactId>
<version>0.6.33</version>
</dependency>
```

> Note: Please check the Maven repository for the latest version.

### Implement ILogHubProcessor and ILogHubProcessorFactory

```java
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.FastLogTag;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;

import java.util.List;

public class SampleLogHubProcessor implements ILogHubProcessor {
    private int shardId;
    // The time when the last checkpoint is saved. 
    private long mLastCheckTime = 0;

    public void initialize(int shardId) {
        this.shardId = shardId;
    }

    // The main logic of data consumption. You must include the code to handle all exceptions that may occur during data consumption. 
    public String process(List<LogGroupData> logGroups,
                          ILogHubCheckPointTracker checkPointTracker) {
        // Display the obtained data. 
        for (LogGroupData logGroup : logGroups) {
            FastLogGroup flg = logGroup.GetFastLogGroup();
            System.out.println("Tags");
            for (int tagIdx = 0; tagIdx < flg.getLogTagsCount(); ++tagIdx) {
                FastLogTag logtag = flg.getLogTags(tagIdx);
                System.out.println(String.format("\t%s\t:\t%s", logtag.getKey(), logtag.getValue()));
            }
            for (int lIdx = 0; lIdx < flg.getLogsCount(); ++lIdx) {
                FastLog log = flg.getLogs(lIdx);
                System.out.println("--------\nLog: " + lIdx + ", time: " + log.getTime() + ", GetContentCount: " + log.getContentsCount());
                for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                    FastLogContent content = log.getContents(cIdx);
                    System.out.println(content.getKey() + "\t:\t" + content.getValue());
                }
            }
        }
        long curTime = System.currentTimeMillis();
        // A checkpoint is written to Simple Log Service at an interval of 30 seconds. If the ClientWorker instance unexpectedly stops within 30 seconds, a newly started ClientWorker instance consumes data from the last checkpoint. A small amount of data may be repeatedly consumed. 
        if (curTime - mLastCheckTime > 30 * 1000) {
            try {
                If true is passed to saveCheckPoint, checkpoints are immediately synchronized to Simple Log Service.If
                false is passed to saveCheckPoint, checkpoints are cached on your computer.By
                default,checkpoints are synchronized to Simple Log Service at an interval of 60 seconds.
                        checkPointTracker.saveCheckPoint(true);
            } catch (LogHubCheckPointException e) {
                e.printStackTrace();
            }
            mLastCheckTime = curTime;
        }
        return null;
    }

    // The shutdown function of the ClientWorker instance is called. You can manage the checkpoints. 
    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        // Save checkpoints to the server. 
        try {
            checkPointTracker.saveCheckPoint(true);
        } catch (LogHubCheckPointException e) {
            e.printStackTrace();
        }
    }
}

class SampleLogHubProcessorFactory implements ILogHubProcessorFactory {
    public ILogHubProcessor generatorProcessor() {
        // Generate a consumer. 
        return new SampleLogHubProcessor();
    }
}
```

## Start ClientWorker instances

```java
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;

public class Main {
    // The Simple Log Service endpoint. Configure the parameter based on your business scenario. 
    private static String Endpoint = "cn-hangzhou.log.aliyuncs.com";
    // The name of the Simple Log Service project. Configure the parameter based on your business scenario. You must enter the name of an existing project. 
    private static String Project = "ali-cn-hangzhou-sls-admin";
    // The name of the Logstore. Configure the parameter based on your business scenario. You must enter the name of an existing Logstore. 
    private static String Logstore = "sls_operation_log";
    // The name of the consumer group. Configure the parameter based on your business scenario. You do not need to create a consumer group in advance. A consumer group is automatically created when a program runs. 
    private static String ConsumerGroup = "consumerGroupX";
    // Configure environment variables. In this example, the AccessKey ID and AccessKey secret are obtained from environment variables.  
    private static String AccessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    private static String AccessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");

    public static void main(String[] args) throws LogHubClientWorkerException, InterruptedException {
        // consumer_1 specifies the name of a consumer. The name of each consumer in a consumer group must be unique. If different consumers start processes on different machines to consume data in a Logstore, you can use the machine IP addresses to identify each consumer. 
        // maxFetchLogGroupSize specifies the maximum number of log groups that can be obtained from Simple Log Service at a time. Retain the default value. You can use config.setMaxFetchLogGroupSize(100); to change the maximum number. Valid range: (0,1000]. 
        LogHubConfig config = new LogHubConfig(ConsumerGroup, "consumer_1", Endpoint, Project, Logstore, AccessKeyId, AccessKeySecret, LogHubConfig.ConsumePosition.BEGIN_CURSOR, 1000);
        ClientWorker worker = new ClientWorker(new SampleLogHubProcessorFactory(), config);
        Thread thread = new Thread(worker);
        // After you execute the thread, the ClientWorker instance automatically runs and extends the Runnable interface. 
        thread.start();
        Thread.sleep(60 * 60 * 1000);
        // The shutdown function of the ClientWorker instance is called to exit the consumption instance. The associated thread is automatically stopped. 
        worker.shutdown();
        // Multiple asynchronous tasks are generated when the ClientWorker instance is running. To ensure that all running tasks securely stop after the shutdown, we recommend that you set Thread.sleep to 30 seconds. 
        Thread.sleep(30 * 1000);
    }
}
```

## Configuration

Below are the main configuration items and explanations for LogHubConfig:

| Attribute                    | Type                 | Default Value                                 | Description                                                                                                                                                                                                                                                                                                                                                                             |
|------------------------------|----------------------|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| consumerGroup                | String               |                                               | Consumer group name.                                                                                                                                                                                                                                                                                                                                                                    |
| consumer                     | String               |                                               | Consumer name.                                                                                                                                                                                                                                                                                                                                                                          |  
| endpoint                     | String               |                                               | Service endpoint. For information on determining the service endpoint for a Project, refer to the [Endpoints](https://www.alibabacloud.com/help/en/sls/developer-reference/endpoints).                                                                                                                                                                                                  |
| project                      | String               |                                               | Name of the project to be consumed.                                                                                                                                                                                                                                                                                                                                                     |                                                                                               
| logstore                     | String               |                                               | Name of the logstore within the project to be consumed.                                                                                                                                                                                                                                                                                                                                 |                                                                                                      
| accessId                     | String               |                                               | AccessKeyId of the account.                                                                                                                                                                                                                                                                                                                                                             |                                                                                                      
| accessKey                    | String               |                                               | AccessKeySecret of the account.                                                                                                                                                                                                                                                                                                                                                         |                                                                                                      
| initialPosition              | LogHubCursorPosition |                                               | The starting point for consumption. This parameter is used only at the time of the first creation of the consumer group. When the consumer group is started again for consumption, it will continue from the last consumed checkpoint. Options include: <br/> - `BEGIN_CURSOR`：Start position<br/> - `END_CURSOR`：End position<br/> - `SPECIAL_TIMER_CURSOR`：Custom start position<br/> |                                                                                                      
| startTimestamp               | int                  |                                               | Custom log consumption time point. This parameter can only be used when `initialPosition` is set to `SPECIAL_TIMER_CURSOR`. It represents the UNIX timestamp in seconds. When the `startTimestamp` parameter is passed, LogHubConfig automatically sets initialPosition to `SPECIAL_TIMER_CURSOR`.                                                                                      |                                                                                                      
| fetchIntervalMillis          | long                 | 200                                           | Interval for fetching logs from the server, in milliseconds. It is recommended to set this value to 200 or more.                                                                                                                                                                                                                                                                        |                                                                                                      
| heartbeatIntervalMillis      | long                 | 5000                                          | The interval for sending heartbeats to the server, in seconds. If the server doesn't receive a heartbeat for (heartbeatIntervalMillis + timeoutInSeconds), it assumes the consumer has gone offline and will reassign the held Shard.                                                                                                                                                   |                                                                                                      
| consumeInOrder               | boolean              | false                                         | Whether to consume logs in order.                                                                                                                                                                                                                                                                                                                                                       |                                                                                                      
| stsToken                     | String               |                                               | AccessKeyToken of the account. Required when consuming data using a role.                                                                                                                                                                                                                                                                                                               |                                                                                                      
| autoCommitEnabled            | boolean              | true                                          | Whether to automatically commit checkpoint information to the server. When enabled, checkpoints will be committed to the server at regular intervals. The interval is configurable through `autoCommitIntervalMs`.                                                                                                                                                                      |                                                                                                      
| batchSize                    | int                  | 1000                                          | The number of log groups to fetch from the server in one request. Default value is 1000, with a range of 1 ~ 1000. For more information on log groups, please refer to [Log Group](https://www.alibabacloud.com/help/en/sls/product-overview/log-group).                                                                                                                                |                                                                                                      
| timeoutInSeconds             | int                  | 60                                            | Consumer timeout period, in seconds.                                                                                                                                                                                                                                                                                                                                                    |                                                                                                      
| maxInProgressingDataSizeInMB | int                  | 0                                             | The maximum amount of data in MB that all Consumers are currently processing. A value of 0 means no limit. If this limit is exceeded, the data fetch thread will be blocked. Therefore, this value can be used to control the rate of asynchronous data fetch and the size of memory used.                                                                                              |                                                                                                      
| userAgent                    | String               | `Consumer-Library-{ConsumerGroup}/{Consumer}` | UserAgent for the API call.                                                                                                                                                                                                                                                                                                                                                             |                                                                                                      
| proxyHost                    | String               |                                               | Proxy server host.                                                                                                                                                                                                                                                                                                                                                                      |                                                                                                      
| proxyPort                    | int                  |                                               | Proxy server port.                                                                                                                                                                                                                                                                                                                                                                      |                                                                                                      
| proxyUsername                | String               |                                               | Proxy server username.                                                                                                                                                                                                                                                                                                                                                                  |                                                                                                      
| proxyPassword                | String               |                                               | Proxy server password.                                                                                                                                                                                                                                                                                                                                                                  |                                                                                                      
| proxyDomain                  | String               |                                               | Proxy server domain.                                                                                                                                                                                                                                                                                                                                                                    |                                                                                                      
| proxyWorkstation             | String               |                                               | Proxy workstation.                                                                                                                                                                                                                                                                                                                                                                      |                                                                                                      


## Common Questions and Precautions

### Introduction to the concept of Consumer Library

There are four main concepts in the Consumer Library: ConsumerGroup, Consumer, Heartbeat, and Checkpoint. Their relationships are as follows:

![](pics/consumer_group_concepts.jpg)

**ConsumerGroup**

A ConsumerGroup is a sub-resource of Logstore. Consumers with the same ConsumerGroup name jointly consume all data from the same Logstore without overlapping data consumption.

A maximum of 30 ConsumerGroups can be created under a single Logstore, and they must have unique names. ConsumerGroups
under the same Logstore do not affect each other's data consumption.

ConsumerGroup has two important attributes:

- `order`: `boolean`, indicates whether to consume data with the same hash key in the order it was written.
- `timeout`: `integer`, the timeout period for consumers within the ConsumerGroup, in seconds. If a consumer's heartbeat interval exceeds the timeout, the server deems the consumer to be offline.

**Consumer**

A Consumer. Multiple Consumers correspond to a single ConsumerGroup, and Consumers within the same ConsumerGroup must not have the same name. Several shards will be allocated to each Consumer, whose responsibility is to consume data on these Shards.

**Heartbeat**

Consumer heartbeat. Consumers must regularly report a heartbeat packet to the server to indicate they are still alive.

**Checkpoint**

Consumption position. Consumers periodically save the consumption position of the Shards assigned to them to the server.

When a Shard is reassigned to another consumer, that new consumer can obtain the consumption checkpoint from the server and continue to consume data from that checkpoint, ensuring no data loss.

### The Relationship Between ConsumerGroup, Consumer, and ClientWorker

In LogHubConfig, ConsumerGroup represents a consumer group. Consumers with the same ConsumerGroup share the consumption of Shards in a Logstore.

Consumer is created and managed by ClientWorker, and there is a one-to-one correspondence between Shard and Consumer.

Assume there are Shards 0 to 3 (4 Shards in total) in a Logstore, and there are 3 Workers with the following ConsumerGroup and Worker pairs:

- `<consumer_group_name_1, worker_A>`
- `<consumer_group_name_1, worker_B>`
- `<consumer_group_name_2, worker_C>`

Then, the possible assignment of Workers to Shards might be:
 
- `<consumer_group_name_1, worker_A>`: shard_0, shard_1
- `<consumer_group_name_1, worker_B>`: shard_2, shard_3
- `<consumer_group_name_2, worker_C>`: shard_0, shard_1, shard_2, shard_3 (Workers with different ConsumerGroups do not affect each other)

### Implementation of ILogHubProcessor

- It is essential to ensure that the implemented `ILogHubProcessor#process()` interface can execute and exit smoothly each time in order to continue fetching the next batch of data.
- If `process()` returns `null` or an empty string, it is considered that the data processing is successful, and the next batch of data will be fetched; otherwise, a Checkpoint must be returned so the Consumer can re-fetch the data corresponding to that Checkpoint.
- For the `saveCheckPoint()` interface of ILogHubCheckPointTracker, whether the passed parameter is true or false, it indicates that the current data processing is complete.
  - If the parameter is `true`, the consumption checkpoint is immediately persisted to the server.
  - If the parameter is `false`, the consumption checkpoint is stored in memory. If autoCommitEnabled is true, the consumption checkpoint will be periodically synchronized to the server.
  
### RAM Permissions for Data Consumption

If the AccessKey of a sub-user or role is configured in LogHubConfig, authorization needs to be completed in RAM. For detailed information, please refer to [RAM User Authorization](https://www.alibabacloud.com/help/en/sls/user-guide/use-consumer-groups-to-consume-data#section-yrp-xfr-7va).

> Note: For security reasons, please use the RAM User Account AccessKey instead of the Alibaba Cloud Account AccessKey.

## Feedback on Issues

If you encounter any issues during use, you can create a [GitHub Issue](https://github.com/aliyun/aliyun-log-consumer-java/issues) or go to the Alibaba Cloud Support Center to [submit a ticket](https://selfservice.console.aliyun.com/service/create-ticket).

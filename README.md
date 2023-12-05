# Consumer Library 使用说明

Aliyun LOG Consumer Library 一个是消费 Logstore 数据的 Java 库，它有如下功能特点：

- 使用简单：在整个使用过程中，不会产生数据丢失和重复，用户只需要进行简单配置、创建消费者实例，然后编写数据处理代码逻辑即可，不需关心消费断点保存，以及错误重试等问题
- 高性能：Consumer 使用多线程异步拉取数据和处理数据，以提高吞吐量和性能
- 自动负载均衡：Consumer 会根据当前 ConsumerGroup 的消费者数量和 Shard 数量自动进行负载均衡，保证任意两个消费者持有 Shard
  数量之差的绝对值小于等于 1
- 自动重试：对程序运行当中出现的可重试的异常，Consumer 会自动重试，重试过程不会导致数据的重复消费
- 线程安全：Consumer 内的所有方法以及暴露的接口都是线程安全的
- 优雅关闭：调用关闭程序接口，Consumer 会等待异步任务结束并将当前消费位点提交至服务端，保证下次开始不会重复消费数据


举个例子，用户需要通过 Storm 进行流计算，启动了 A、B、C 3 个消费实例。在有 10 个 Shard 情况下，系统会自动为 A、B、C 分配 3、3、4 个
Shard 进行消费。部分示例场景如下：

1. 场景一：消费实例 A 宕机，则：系统会把 A 未消费的 3 个 Shard 中数据自动均衡 B、C 上，当 A 恢复后，会重新均衡； 
2. 场景二：添加实例 D、E，则：系统会自动进行均衡，每个实例消费 2 个 Shard； 
3. 场景三：Shard 进行分裂或合并，则：系统会根据最新的 Shard 信息，重新均衡； 
4. 场景三：只读（readonly）状态的 Shard 消费完毕，则：剩余的 Shard 会重新做负载均衡。

以上整个过程不会产生数据丢失、以及重复，自动处理 Shard 的负载均衡、消费者故障恢复等逻辑。用户只需专注在自己业务逻辑上，而无需关心
Shard 分配、CheckPoint、Failover 等事宜。


## 实现原理


Consumer Library 中主要有 4 个概念，分别是 ConsumerGroup、Consumer、Heartbeat 和 Checkpoint，它们之间的关系如下：

![](pics/consumer_group_concepts.jpg)

#### ConsumerGroup

消费组。ConsumerGroup 是 Logstore 的子资源，拥有相同 ConsumerGroup 名字的消费者共同消费同一个 Logstore
的所有数据，这些消费者之间不会重复消费数据。

一个 Logstore 下面可以最多创建 30 个 ConsumerGroup，不可以重名。同一个 Logstore 下的 ConsumerGroup 之间消费数据互不相影响。

ConsumerGroup 有两个很重要的属性：

- `order`：`boolean`，表示是否按照写入时间顺序消费 hash key 相同的数据；
- `timeout`：`integer`，表示 ConsumerGroup 中消费者的超时时间，单位秒。当一个消费者汇报心跳的时间间隔超过
  timeout，则服务端会认为该消费者已经下线。

#### Consumer

消费者。一个 ConsumerGroup 对应多个 Consumer，同一个ConsumerGroup 中的 Consumer 不能重名。每个 Consumer 上会被分配若干个
Shard，Consumer 的职责就是要消费这些 Shard 上的数据。

#### Heartbeat

消费者心跳。Consumer 需要定期向服务端汇报一个心跳包，用于表明自己还处于存活状态。

#### Checkpoint

消费位点。消费者定期将分配给自己的 Shard 的消费位点保存到服务端，这样当该 Shard 被分配给其它消费者时，其他消费者就可以从服务端获取
Shard 的消费断点，接着从断点继续消费数据，进而保证数据不丢失。

## 如何使用

使用 Consumer Library 主要分为三步：

1. 添加依赖；
2. 实现 Consumer Library 中的两个接口，编写业务逻辑：
- `ILogHubProcessor`：每个 Shard 对应一个实例，每个实例只消费特定 Shard 的数据；
- `ILogHubProcessorFactory`：负责生成实现 ILogHubProcessor 的接口实例；
3. 启动一个或多个 ClientWorker 实例。

### 添加依赖

以 maven 为例，在 `pom.xml` 中添加如下依赖：

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

> 注意：请到 maven 仓库中查看最新版本。

### 实现 ILogHubProcessor 和 ILogHubProcessorFactory

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
    // 记录上次持久化 Checkpoint 的时间。
    private long mLastCheckTime = 0;

    public void initialize(int shardId) {
        this.shardId = shardId;
    }

    // 消费数据的主逻辑，消费时的所有异常都需要处理，不能直接抛出。
    public String process(List<LogGroupData> logGroups,
                          ILogHubCheckPointTracker checkPointTracker) {
        // 打印已获取的数据。
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
        // 每隔 30 秒，写一次Checkpoint到服务端。如果 30 秒内 Worker 发生异常终止，新启动的 Worker 会从上一个 Checkpoint 获取消费数据，可能存在少量的重复数据。
        if (curTime - mLastCheckTime > 30 * 1000) {
            try {
                // 参数为 true 表示立即将 Checkpoint 更新到服务端；false 表示将 Checkpoint 缓存在本地。默认间隔60秒会将 Checkpoint 更新到服务端。
                checkPointTracker.saveCheckPoint(true);
            } catch (LogHubCheckPointException e) {
                e.printStackTrace();
            }
            mLastCheckTime = curTime;
        }
        return null;
    }

    // 当 Worker 退出时，会调用该函数，您可以在此处执行清理工作。
    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        // 将Checkpoint立即保存到服务端。
        try {
            checkPointTracker.saveCheckPoint(true);
        } catch (LogHubCheckPointException e) {
            e.printStackTrace();
        }
    }
}

class SampleLogHubProcessorFactory implements ILogHubProcessorFactory {
    public ILogHubProcessor generatorProcessor() {
        // 生成一个消费实例。
        return new SampleLogHubProcessor();
    }
}
```

### 启动一个或多个 ClientWorker 实例

```java
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;

public class Main {
    // 日志服务的服务接入点，请您根据实际情况填写。
    private static String Endpoint = "cn-hangzhou.log.aliyuncs.com";
    // 日志服务项目名称，请您根据实际情况填写。请从已创建项目中获取项目名称。
    private static String Project = "ali-cn-hangzhou-sls-admin";
    // 日志库名称，请您根据实际情况填写。请从已创建日志库中获取日志库名称。
    private static String Logstore = "sls_operation_log";
    // 消费组名称，请您根据实际情况填写。您无需提前创建，该程序运行时会自动创建该消费组。
    private static String ConsumerGroup = "consumerGroupX";
    // 本示例从环境变量中获取AccessKey ID和AccessKey Secret。。
    private static String AccessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    private static String AccessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");

    public static void main(String[] args) throws LogHubClientWorkerException, InterruptedException {
        // consumer_1 是消费者名称，同一个消费组下面的消费者名称必须不同。不同消费者在多台机器上启动多个进程，均衡消费一个 Logstore 时，消费者名称可以使用机器IP地址来区分。
        // maxFetchLogGroupSize 用于设置每次从服务端获取的 LogGroup 最大数目，使用默认值即可。您可以使用 config.setMaxFetchLogGroupSize(100) 调整，取值范围为(0,1000]。
        LogHubConfig config = new LogHubConfig(ConsumerGroup, "consumer_1", Endpoint, Project, Logstore, AccessKeyId, AccessKeySecret, LogHubConfig.ConsumePosition.BEGIN_CURSOR, 1000);
        ClientWorker worker = new ClientWorker(new SampleLogHubProcessorFactory(), config);
        Thread thread = new Thread(worker);
        // Thread 运行之后，ClientWorker 会自动运行，ClientWorker 扩展了 Runnable 接口。
        thread.start();
        Thread.sleep(60 * 60 * 1000);
        // 调用 Worker 的 shutdown 函数，退出消费实例，关联的线程也会自动停止。
        worker.shutdown();
        // ClientWorker 运行过程中会生成多个异步的任务。shutdown 完成后，请等待还在执行的任务安全退出。建议设置 sleep 为 30 秒。
        Thread.sleep(30 * 1000);
    }
}
```

## 配置说明

LogHubConfig 主要配置项及说明如下：

| 属性                           | 类型                   | 默认值                                           | 描述                                                                                                                                                                                                                                   |
|------------------------------|----------------------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| consumerGroup                | String               |                                               | 消费组名称                                                                                                                                                                                                                                |
| consumer                     | String               |                                               | 消费者名称                                                                                                                                                                                                                                |  
| endpoint                     | String               |                                               | 服务入口，关于如何确定 Project 对应的服务入口可参考文章[服务入口](https://help.aliyun.com/zh/sls/developer-reference/endpoints)                                                                                                                                 |
| project                      | String               |                                               | 将要消费的项目名称                                                                                                                                                                                                                            |                                                                                                      |
| logstore                     | String               |                                               | 将要消费的项目下的日志库名称                                                                                                                                                                                                                       |                                                                                                      |
| accessId                     | String               |                                               | 云账号的 AccessKeyId                                                                                                                                                                                                                     |                                                                                                      |
| accessKey                    | String               |                                               | 云账号的 AccessKeySecret                                                                                                                                                                                                                 |                                                                                                      |
| initialPosition              | LogHubCursorPosition | 依构造函数而定                                       | 开始消费的时间点，该参数只在第一次创建消费组的时候使用，当再次启动消费组进行消费的时候会从上次消费到的断点进行继续消费。可选值： <br/> - `BEGIN_CURSOR`：开始位置<br/> - `END_CURSOR`：结束位置<br/> - `SPECIAL_TIMER_CURSOR`：自定义起始位置<br/> > LogHubConfig 构造函数之一的参数为 position，类型是 LogHubConfig.ConsumePosition |                                                                                                      |
| startTimestamp               | int                  | 依构造函数而定                                       | 自定义日志消费时间点，只有当 initialPosition 设置为 `SPECIAL_TIMER_CURSOR` 时，该参数才能使用，参数为 UNIX 时间戳，单位为秒。 > LogHubConfig 构造函数之一的参数为 startTimestamp，传入该参数时，  LogHubConfig 会自动将 initialPosition 设置为 `SPECIAL_TIMER_CURSOR`                                |                                                                                                      |
| fetchIntervalMillis          | long                 | 200                                           | 服务端拉取日志时间间隔，单位毫秒，建议取值 200 以上                                                                                                                                                                                                         |                                                                                                      |
| heartbeatIntervalMillis      | long                 | 5000                                          | 向服务端发送的心跳间隔，单位秒。如果超过（heartbeatIntervalMillis + timeoutInSeconds）没有向服务端汇报心跳，服务端就认为该消费者已经掉线，会将该消费者持有的 Shard 进行重新分配                                                                                                                     |                                                                                                      |
| consumeInOrder               | boolean              | false                                         | 是否按序消                                                                                                                                                                                                                                |                                                                                                      |
| stsToken                     | String               |                                               | 云账号的 AccessKeyToken。基于角色扮演的身份消费数据时，需要该属性                                                                                                                                                                                             |                                                                                                      |
| directModeEnabled            | boolean              | false                                         |                                                                                                                                                                                                                                      |                                                                                                      |
| autoCommitEnabled            | boolean              | true                                          | 是否自动提交消费位点到服务端。 开启后，会每隔一定时间自动提交消费位点到服务端。间隔时间可通过 autoCommitIntervalMs 配置                                                                                                                                                              |                                                                                                      |
| unloadAfterCommitEnabled     | boolean              | false                                         | 当 shard 的消费位点被提交后，是否销毁 Consumer                                                                                                                                                                                                      |                                                                                                      |
| autoCommitIntervalMs         | long                 | 60000                                         | 自动提交消费位点的间隔时间，单位毫秒。当 autoCommitEnabled 为 true 时，该配置有效                                                                                                                                                                                |                                                                                                      |
| batchSize                    | int                  | 1000                                          | 从服务端一次拉取日志组数量，日志组可参考内容[日志组](https://help.aliyun.com/zh/sls/product-overview/log-group)，默认值 1000，其取值范围是 1 ~ 1000                                                                                                                      |                                                                                                      |
| timeoutInSeconds             | int                  | 60                                            | 表示消费者的超时时间，单位秒                                                                                                                                                                                                                       |                                                                                                      |
| maxInProgressingDataSizeInMB | int                  | 0                                             | 所有 Consumer 正在处理的最大数据量，单位 MB。0 表示不限制。超过限制后，会阻塞拉取线程。所以可以通过该值控制异步拉取数据的速率和内存大小                                                                                                                                                          |                                                                                                      |
| userAgent                    | String               | `Consumer-Library-{ConsumerGroup}/{Consumer}` | 调用接口的 UserAgent                                                                                                                                                                                                                      |                                                                                                      |
| proxyHost                    | String               |                                               | 代理服务器地址                                                                                                                                                                                                                              |                                                                                                      |
| proxyPort                    | int                  |                                               | 代理服务器端口                                                                                                                                                                                                                              |                                                                                                      |
| proxyUsername                | String               |                                               | 代理服务器用户名                                                                                                                                                                                                                             |                                                                                                      |
| proxyPassword                | String               |                                               | 代理服务器密码                                                                                                                                                                                                                              |                                                                                                      |
| proxyDomain                  | String               |                                               | 代理服务器域名                                                                                                                                                                                                                              |                                                                                                      |
| proxyWorkstation             | String               |                                               | 代理工作站                                                                                                                                                                                                                                |                                                                                                      |

## 常见问题及注意事项

### ConsumerGroup、Consumer 和 ClientWorker 的关系

LogHubConfig 中 ConsumerGroup 表一个消费组，ConsumerGroup 相同的 Consumer 分摊消费 Logstore 中的 Shard。

Consumer 由 ClientWorker 创建和管理，Shard 和 Consumer 一一对应。

假设 Logstore 中有 Shard 0 ~ Shard 3 这 4 个 Shard ，有 3个 Worker，其 ConsumerGroup 和 Worker 分别是：

- <consumer_group_name_1 , worker_A>
- <consumer_group_name_1 , worker_B>
- <consumer_group_name_2 , worker_C>

则，这些 Worker 和 Shard 的分配关系可能是：

- <consumer_group_name_1 , worker_A>: shard_0, shard_1
- <consumer_group_name_1 , worker_B>: shard_2, shard_3
- <consumer_group_name_2 , worker_C>: shard_0, shard_1, shard_2, shard_3 （ConsumerGroup 不同的 Worker 互不影响）

### ILogHubProcessor 的实现

- 需要确保实现的 ILogHubProcessor `process()` 接口每次都能顺利执行并退出，这样才能继续拉取下一批数据
- 如果 `process()` 返回 `null` 或空字符串，则认为数据处理成功，会继续拉取下一批数据；否则必须返回 Checkpoint，以便 Consumer 重新拉取对应 Checkpoint 的数据
- ILogHubCheckPointTracker的 `saveCheckPoint()` 接口，无论传递的参数是 true 或 false，都表示当前处理的数据已经完成
    - 参数为 `true`，则立刻将消费位点持久化至服务端
    - 参数为 `false`，则会将消费位点存储在内存。如果 `autoCommitEnabled` 为 `true`，会定期将消费位点同步到服务端

### RAM 权限

LogHubConfig 中配置的如果是子用户或角色的 AccessKey，需要在 RAM 中进行授权，详细内容请参考 [RAM用户授权](https://help.aliyun.com/zh/sls/user-guide/use-consumer-groups-to-consume-data#section-yrp-xfr-7va)。

> 注意：为了安全起见，请不要使用主账号 AccessKey。

## 问题反馈

如果您在使用过程中遇到了问题，可以创建 [GitHub Issue](https://github.com/aliyun/aliyun-log-consumer-java/issues) 或者前往阿里云支持中心[提交工单](https://selfservice.console.aliyun.com/service/create-ticket)。
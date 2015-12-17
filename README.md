# Loghub Client worker lib说明文档
LogHub client worker提供一个LogHub数据分布式消费框架，用户只需要提供LogHub数据处理逻辑，其他逻辑全部由框架负责，主要包括：
* 从LogHub抓取数据
* 调用用户实现的数据处理逻辑
* 多个消费者之间协同，完成负载均衡，确保任意时刻，一个LogStream的任意shard有且只有一个消费实例
* 提供checkpoint接口，确保在failover的情况下，不会出现数据漏消费的情况

## 应用需要完成的事情
为了能够分布式消费Loghub中的数据，应用方需要完成以下事情：
* 实现Loghub client worker的两个接口类 :
    * ILogHubProcessor // 每个shard对应一个实例，每个实例只消费特定shard的数据。
    * ILogHubProcessorFactory // 负责生产实现ILogHubProcessor接口实例
* 提供配置 
* 启动一个或多个client worker

## 使用sample 
```
	public static void main(String args[]) {
		LogHubConfig config = new LogHubConfig(...);
				
		ClientWorker worker = new ClientWorker(new SampleLogHubProcessorFactory(), config),
		
		Thread thread = new Thread(worker);
		thread.start();
		worker.shutdown();
		Thread.sleep(30 * 1000);
	}

```

## ILogHubProcessor、ILogHubProcessorFactory 实现sample

* 各个shard对应的消费实例类 ：
```
public class SampleLogHubProcessor implements ILogHubProcessor {
	private String mShardId;
	private long mLastCheckTime = 0; // 记录上次持久化check point的时间
	
	public void initialize(String shardId) {
		mShardId = shardId;
	}

	// 消费数据的主逻辑
	public String process(List<LogGroup> logGroups,
			ILogHubCheckPointTracker checkPointTracker) {
		for (LogGroup group : logGroups) {
			List<LogItem> items = group.getAllLogs();
			for (LogItem item : items) {
			    // 打印loggroup中的数据
				System.out.println("shard_id:" + mShardId + " " + item.toJSONString());
			}
		}
		long curTime = System.currentTimeMillis();
		// 每隔60秒，写一次check point到mysql中，如果60秒内，worker crash，
		// 新启动的worker会从上一个checkpoint其消费数据，有可能有重复数据
		if (curTime - mLastCheckTime >  60 * 1000) {
			try {
				checkPointTracker.saveCheckPoint(true);
			} catch (LogHubCheckPointException e) {
				e.printStackTrace();
			}
			mLastCheckTime = curTime;
		} else {
			try {
				checkPointTracker.saveCheckPoint(false);
			} catch (LogHubCheckPointException e) {
				e.printStackTrace();
			}
		}
		return "";  // 返回空表示正常处理数据， 如果需要回滚到上个check point的点进行重试的话，可以return checkPointTracker.getCheckpoint()
	}
	
	public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
	    checkPointTracker.saveCheckPoint(true);
	}
```

* 生成 ILogHubProcessor的工厂类 ：
```
public class SampleLogHubProcessorFactory implements ILogHubProcessorFactory {
	public ILogHubProcessor generatorProcessor()
	{   
	    // 生成一个消费实例
		return new SampleLogHubProcessor();
	}
}
```

## 配置说明：

```
public class LogHubConfig {
	public static final long DEFAULT_DATA_FETCH_INTERVAL_MS = 500;
	private String mConsumerGroupName;
	private String mWorkerInstanceName;
	private String mLogHubEndPoint;
	private String mProject;
	private String mLogStore;
	private String mAccessId;
	private String mAccessKey;
	private LogHubCursorPosition mCursorPosition;
	private int  mLoghubCursorStartTime = 0;
	private long mDataFetchIntervalMillis;// 轮询获取loghub的时间间隔，间隔越小，抓取越快，单位是ms
	private long mHeartBeatIntervalMillis;
	private boolean mConsumeInOrder; //是否按序消费
}
```
## Maven配置
```
<dependency>
  <groupId>com.aliyun</groupId>
  <artifactId>sls-loghub-client-inner</artifactId>
  <version>0.1.4</version>
</dependency>
```

## 常见问题&注意事项
1. LogHubConfig 中 consumerGroupName表一个消费组，consumerGroupName相同的worker分摊消费logstore中的shard数据，同一个consumerGroupName中的worker，通过workerInstance name进行区分。 
```
   假设logstore中有shard 0 ~ shard 3 这4个shard。
   有3个worker，其consumerGroupName和workerinstance name分别是 : 
   worker 1  : <consumer_group_name_1 , worker_A>
   worker 2  : <consumer_group_name_1 , worker_B>
   worker 3  : <consumer_group_name_2 , worker_C>
   则，这些worker和shard的分配关系是：
   worker 1  : <consumer_group_name_1 , worker_A>   : shard_0, shard_1
   worker 2  : <consumer_group_name_1 , worker_B>   : shard_2, shard_3
   worker 3  : <consumer_group_name_2 , worker_C>   : shard_0, shard_1, shard_2, shard_3  # group name不同的worker相互不影响
```
2. 确保实现的process（）接口每次都能顺利执行，并退出，这点很重要
3. ILogHubCheckPointTracker的saveCheckPoint（）接口，无论传递的参数是true，还是false，都表示当前处理的数据已经完成，参数为true，则立刻持久化至数据库，false则每隔60秒同步一次到数据库
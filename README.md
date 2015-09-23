# Loghub Client worker lib说明文档
LogHub client worker提供一个LogHub数据分布式消费框架，用户只需要提供LogHub数据处理逻辑，其他逻辑全部由框架负责，主要包括：
* 从LogHub抓取数据
* 调用用户实现的数据处理逻辑
* 多个消费者之间协同，完成负载均衡，确保任意时刻，一个LogStream的任意shard有且只有一个消费实例
* 提供checkpoint接口，确保在failover的情况下，不会出现数据漏消费的情况

## 应用需要完成的事情
为了能够分布式消费Loghub中的数据，应用方需要完成以下事情：
* 提供一个mysql实例，用于持久化shard同步信息，以及checkpoint
* 实现Loghub client worker的两个接口类 :
    * ILogHubProcessor // 每个shard对应一个实例，每个实例只消费特定shard的数据。
    * ILogHubProcessorFactory // 负责生产实现ILogHubProcessor接口实例
* 提供配置 
* 启动一个或多个client worker

## 使用sample 
```
	public static void main(String args[]) {

	    // 数据库的配置，用户多worker协同消费一个logstream，以及持久化check point
		LogHubClientDbConfig dbConfig = new LogHubClientDbConfig(
				$mysql_host, $mysql_port, $mysql_database, $mysql_user, $mysql_db,
				// 提供两个数据库的表来保存基本信息
				$mysql_worker_instance_table_name, $mysql_shard_lease_table_name); 

        // 需要消费的logHub中的project和logstream
		String lobhub_project =  ...
		String loghub_logstream ...
		
		// $loghub_consume_group是一个<$lobhub_project,$loghub_logstream>的消费分组
		// 所有$loghub_consume_group相同的进程将协同一起消费一个logstream的数据
		String loghub_consume_group = ...
		 // 每个进程的instance name必须不同
		String instanceName = $your_instance_name;
		// 当找不到checkpoint或者checkpoint失效的情况下（超过生命周期），是从头，还是从尾部开始消费数据
        LogHubCursorPosition init_cursor = LogHubCursorPosition.END_CURSOR
        
		LogHubConfig config = new LogHubConfig($loghub_consume_group, $instanceName,
				$loghub_enpoint, $loghub_project, $loghub_logstream,
				$access_id, $access_key,
				$init_cursor);    // 如果需要拉取特定时间点之后的数据，最后一个参数也可以填写成time_stamp（精确到秒）
				                  // 如(int)(curTime - 3600)， 表示从一个小时之前开始拉数据
				
		config.setDataFetchIntervalMillis(1000); // 设置每个shard从loghub中抓取的时间间隔，单位是毫秒
		
		ILogHubLeaseManager leaseManager = new MySqlLogHubLeaseManager(dbConfig);  //构建一个用于shard租赁的manager
				
		ClientWorker worker = new ClientWorker(
				new SampleLogHubProcessorFactory(),  // 用户实现的processor工厂类，用于为每个shard生成一个Processor
				config， leaseManager);
		
		//启动 worker
		worker.run();
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
	public static final long DEFAULT_LEASE_DURATION_TIME_MS = 15 * 1000; // shard租赁周期，默认15秒，如果15秒内shard没有更新lease，其他worker可以抢占该lease
	public static final long DEFAULT_DATA_FETCH_INTERVAL_MS = 500; // 默认500ms为每个shard抓取一次数据
	private String mConsumeGroupName;
	private String mWorkerInstanceName;
	private String mLogHubEndPoint;
	private String mLogHubProject;
	private String mLogHubStreamName;
	private String mAccessId;
	private String mAccessKey;
	private LogHubClientDbConfig mDbConfig;
	private LogHubCursorPosition mCursorPosition;
	private long mLeaseDurationMillis;
	private long mDataFetchIntervalMillis;  // 轮询获取loghub的时间间隔，间隔越小，抓取越快，单位是ms
}
```
## Maven配置
```
<dependency>
  <groupId>com.aliyun</groupId>
  <artifactId>sls-loghub-client-inner</artifactId>
  <version>0.1.3</version>
</dependency>
```

## 常见问题&注意事项
1. LogHubConfig 中 consumerGroupName表一个消费组，consumerGroupName相同的worker分摊消费logstore中的shard数据，同一个consumerGroupName中的worker，通过workerInstance name进行区分。 
```
   假设logstore中有shard 0 ~ shard 4 这4个shard。
   有3个worker，其consumerGroupName和workerinstance name分别是 : 
   worker 1  : <consumer_group_name_1 , worker_A>
   worker 2  : <consumer_group_name_1 , worker_B>
   worker 3  : <consumer_group_name_2 , worker_C>
   则，这些worker和shard的分配关系是：
   worker 1  : <consumer_group_name_1 , worker_A>   : shard_0, shard_1
   worker 2  : <consumer_group_name_1 , worker_B>   : shard_2, shard_3
   worker 3  : <consumer_group_name_2 , worker_C>   : shard_0, shard_1, shard_2, shard_3  # group name不同的worker相互不影响
```

2. 确保数据库的能够正常连接（权限，网络问题），LoghubClient强依赖数据库的可用性
3. 如果连接数据库的延时太高、worker cpu太高，可能导致shard lease更新失败，而导致丢锁，进而导致worker对shard进行shut down操作。可增加进程来降低worker的cpu消耗
4. LogHubConfig的setLeaseDurationTimeMillis()函数可以设置同步至数据库的时间间隔，默认为45000毫秒，如果数据库连接不稳定的话，可以设置成120 * 1000毫秒
5. 确保实现的process（）接口每次都能顺利执行，并退出，这点很重要
6. ILogHubCheckPointTracker的saveCheckPoint（）接口，无论传递的参数是true，还是false，都表示当前处理的数据已经完成，参数为true，则立刻持久化至数据库，false则每隔60秒同步一次到数据库
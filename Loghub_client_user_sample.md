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
				$loghub_enpoint, $loghub_port, $loghub_project, $loghub_logstream,
				$access_id, $access_key, dbConfig,
				$init_cursor);  
				
		config.setDataFetchIntervalMillis(1000); // 设置每个shard从loghub中抓取的时间间隔，单位是毫秒
		ClientWorker worker = new ClientWorker(
				new SampleLogHubProcessorFactory(),  // 用户实现的processor工厂类，用于为每个shard生成一个Processor
				config);
		
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
	public void process(List<LogGroup> logGroups,
			ILogHubCheckPointTracker checkPointTracker) {
		for (LogGroup group : logGroups) {
			ArrayList<JSONObject> objs = group.getAllLogs();
			for (JSONObject obj : objs) {
			    // 打印loggroup中的数据
				System.out.println("shard_id:" + mShardId + " " + obj.toString());
			}
		}
		long curTime = System.currentTimeMillis();
		// 每隔60秒，写一次check point到外部系统，其他时刻，只将check point记录在内存
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
	private int mLogHubPort;
	private String mLogHubProject;
	private String mLogHubStreamName;
	private String mAccessId;
	private String mAccessKey;
	private LogHubClientDbConfig mDbConfig;
	private LogHubCursorPosition mCursorPosition;
	private long mLeaseDurationMillis;
	private long mDataFetchIntervalMillis;
}
```
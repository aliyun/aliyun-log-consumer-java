# loghub client library使用说明

## 使用场景
如果logstore中的数据量比较大，shard数量很多，单一进程消费数据的速度追赶不上数据产生的速度，这个时候就会考虑使用多个消费进程协同消费logstore中的数据，这样自然会有以下问题：
1. 怎么保证多个进程之间消费的数据不会有重叠，比如shard 0的数据不能同时被进程A和B消费。
2. 当消费进程退出或者一个新的消费进程加入，怎么做数据消费的负载均衡，比如有4个进程，每个进程消费2个shard，当其中一个进程退出之后，需要将原本由其负责消费的2个shard均摊到其他的3个进程上，并且数据消费要从shard上一次的消费断点开始。
3. shard之间可能会有父子关系，比如shard 0 分裂成1和2，这个时候shard 0将不会再有数据写入，我们希望shard 0中的数据被消费完之后再消费shard 1和2中的数据，这样的场景概括起来就是希望key相同的数据能够按照写入的时间顺序被消费。当然，如果不关心key相同数据的消费顺序，shard 0、1、2可以同时消费。

上面三点就是loghub client library的设计初衷，综合起来主要是消费的负载均衡、消费断点保存、按序消费。我们强烈建议使用loghub client library进行数据消费，这样您只需要关心怎么处理数据，而不需要关注复杂的负载均衡、消费断点保存、按序消费等问题。

## 术语简介

* consumer group

是logstore的子资源，拥有相同consumer group 名字的消费者共同消费同一个logstore的所有数据，这些消费者之间不会重复消费数据，一个logstore下面可以最多创建5个consumer group，不可以重名，同一个logstore下面的consumer group之间消费数据不会互相影响。consumer group有两个很重要的属性：
```
{
	"order":boolean,
	"timeout": integer
}
```
order属性表示是否按照写入时间顺序消费key相同的数据，timeout表示consumer group中消费者的超时时间，单位是秒，当一个消费者汇报心跳的时间间隔超过timeout，会被认为已经超时，服务端认为这个consumer此时已经下线了。
* consumer

消费者，每个consumer上会被分配若干个shard，consumer的职责就是要消费这些shard上的数据，同一个consumer group中的consumer必须不重名。

* consumer heartbeat

消费者心跳，consumer需要定期向服务端汇报一个心跳包，用于表明自己还处于存活状态。
* checkpoint

消费者定期将分配给自己的shard消费到的位置保存到服务端，这样当这个shard被分配给其它消费者时，从服务端可以获取shard的消费断点，接着从断点继续消费数据。

## 接口说明
loghub client library基于以下服务端提供的接口实现，目前只实现了java版本的loghub client library，这部分不影响您对loghub client library的使用，可以跳过：
```java
/*
	创建consumr group，inOrder表示是否希望key相同的数据能够按照写入的时间顺序被消费，
	timeoutInSec表示consumer的心跳超时时间，超过timeoutInSec没有汇报心跳的consumer会被认为已经下线了，建议取值20s左右。
*/
boolean CreateConsumerGroup(
			String project, 
			String logstore, 
			String consumerGroupName, 
			boolean inOrder,
			int timeoutInSec);
/*
	删除consumer group
*/
boolean DeleteConsumerGroup(
			String project, 
			String logStore, 
			String consumerGroup);
/*
	列出logstore下所有的consumer group，包括每个consumer group的order、timeout属性
*/
List<ConsumerGroup> ListConsumerGroup(
			String project, 
			String logStore);
/*
	更新consumer group的属性，consumer group的名字不可以更新，如果将inOrder由true更新为false，
	那么当前所有未开始消费的shard将会被分配至各个consumer，如果inOrder由false更新为true，对于当前正在消费的shard不会生效，
	也就是说即使shard之间有父子关系，由于它们都已经被消费了，所以顺序对他们而言没有意义，但是inOrder属性更新之后，shard分裂和合并产生的新的shard将会被顺序消费。
*/
boolean UpdateConsumerGroup(
			String project,
			String logStore,
			String consumerGroup,
			boolean inOrder,
			int timeoutInSec
			);
/*
	更新shard消费到的位置，只有当参数中的shard当前由consumer持有的情况下才能更新成功
*/
boolean UpdateCheckPoint(
			String project, 
			String logStore, 
			String consumerGroup, 
			String consumer,
			int shard,
			String checkpoint);
/*
	更新shard消费到的位置，无论如何都可以更新成功
*/
boolean UpdateCheckPoint(
			String project, 
			String logStore, 
			String consumerGroup, 
			int shard,
			String checkpoint);
/*
	获取consumer group中shard的消费断点
*/
String GetCheckPoint(
			String project,
			String logStore,
			String consumerGroup,
			int shard);
/*
	获取consumer group中所有shard的消费断点，返回结果是一个断点的List，具体请参考sdk
*/
List<ShardCheckPoint> GetCheckPoint(
			String project,
			String logStore,
			String consumerGroup);
/*
	将当前consumer持有的shard汇报给服务端，服务端返回确认包，里面包含若干shard，假设持有的shard集合为A，返回的确认shard集合为B，
	A和B的差集(A-B)表示consumer应该放弃消费的shard，consumer应该尽快将(A-B)中的shard的消费断点保存到服务端，并放弃消费(A-B)中的shard，
	(B-A)表示consumer可以增持的shard，consumer从服务端获取(B-A)中shard的checkpoint。理想状态下下一次心跳汇报给服务端的shard集合就是B，
	但是如果consumer不想(没来得及)放弃A-B中的某些shard，需要将这些shard一并汇报给服务端。
	Heartbeat除了上面有消费负载均衡的功能以外，还用于告知服务端自己处于存活状态，不要将自己从consumer group中删除。一旦heartbeat超时，
	服务端就会将consumer从consumer group中删除，并将分配给其的shard重新分配给别的consumer。
*/
List<Integer> HeartBeat(
			String project,
			String logStore,
			String consumerGroup,
			String consumer,
			ArrayList<Integer> shards
			)

```
## 如何使用loghub client library

* 实现loghub client library中的两个接口类：
	* ILogHubProcessor // 每个shard对应一个实例，每个实例只消费特定shard的数据。
	* ILogHubProcessorFactory // 负责生产实现ILogHubProcessor接口实例。
* 填写参数配置。 
* 启动一个或多个client worker实例。

## 使用sample 

### main函数

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
### ILogHubProcessor、ILogHubProcessorFactory 实现sample
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
		// 每隔60秒，写一次check point到服务端，如果60秒内，worker crash，
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
		// 返回空表示正常处理数据， 如果需要回滚到上个check point的点进行重试的话，可以return checkPointTracker.getCheckpoint()
		return null;  
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
### 配置说明：

```
public class LogHubConfig {
    //worker默认的拉取数据的时间间隔
	public static final long DEFAULT_DATA_FETCH_INTERVAL_MS = 500;
	//consumer group的名字
	private String mConsumerGroupName;
	//consumer的名字，必须确保同一个consumer group下面的各个consumer不重名
	private String mWorkerInstanceName;
	//loghub数据接口地址
	private String mLogHubEndPoint;
	//项目名称
	private String mProject;
	//日志库名称
	private String mLogStore;
	//云账号的access key id
	private String mAccessId;
	//云账号的access key
	private String mAccessKey;
	//用于指出在服务端没有记录shard的checkpoint的情况下应该从什么位置消费shard，取值可以是[BEGIN_CURSOR, END_CURSOR, SPECIAL_TIMER_CURSOR]中的一个
	private LogHubCursorPosition mCursorPosition;
	//当mCursorPosition取值为SPECIAL_TIMER_CURSOR时，指定消费时间
	private int  mLoghubCursorStartTime = 0;
	// 轮询获取loghub数据的时间间隔，间隔越小，抓取越快，单位是ms，默认是DEFAULT_DATA_FETCH_INTERVAL_MS
	private long mDataFetchIntervalMillis;
	// worker想服务端汇报心跳的时间间隔，单位是毫秒
	private long mHeartBeatIntervalMillis;
	//是否按序消费
	private boolean mConsumeInOrder; 
}
```
### Maven配置
```
<dependency>
  <groupId>com.aliyun</groupId>
  <artifactId>sls-loghub-client-inner</artifactId>
  <version>0.1.4</version>
</dependency>
```
## 常见问题&注意事项
1. LogHubConfig 中 consumerGroupName表一个消费组，consumerGroupName相同的consumer分摊消费logstore中的shard数据，同一个consumerGroupName中的consumer，通过workerInstance name进行区分。 
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
3. ILogHubCheckPointTracker的saveCheckPoint（）接口，无论传递的参数是true，还是false，都表示当前处理的数据已经完成，参数为true，则立刻持久化至服务端，false则每隔60秒同步一次到服务端。
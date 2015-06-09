# LogHub Client同步方案
LogHub是一个日志的存储、消费系统，提供海量日志的临时存储，一个或多个用户可通过LogHub提供的restful API共同来消费写入LogHub中的日志。为了提供海量日志存储，LogHub为每个LogStream（类似kafka的topic）提供了多个shard（类似partition）来支持日志的并行写入和读取。当用户读取一个LogStream的日志的时候，需要显示指定是从哪个shard读取日志。

## 1. 问题
LogHub的消费者，绝大部分情况下，都是在分布式环境下进行的，用户往往在多个机器上，启动多个进程共同消费这些shard中的数据。当多进程需用共同消费这些数据的时候，必然会遇到以下几个问题：
* 多个消费者如何同步消费shard中的数据，如何保证任意时刻Logstream中的一个shard有且只有一个进程在消费
* 当消费进程failover之后（进程crash，机器宕机），被该进行消费的shard能无缝迁移到其他进程上
* 如何动态负载均衡，当shard个数变化或LogStream中数据量增大的情况下，如何保证在不中断消费过程，通过增加消费进程完成负责均衡

## 2. LogHub Client Lib的目标
针对以上问题，LogHub提供一个client lib，client lib内部完成自动的load balance，fail over处理，使得应用方只需要focuse在数据处理上即可。

## 3. 实现方案
* Client实现shard 租赁协议
* Client依赖一个外部系保存租赁状态
* Client根据通过计算当前shard个数、instance worker个数、租赁状态信息，通过续租、抢占shard的方式，来达到load balance
* 提供一个数据处理接口，应用层实现内部逻辑，以及触发check point

## 4. 功能 & 限制
### 4.1 功能
* 支持LogStream shard个数动态变化 
* 支持worker instance个数动态变化
* client lib自动完成负载均衡，在负载均衡中保证数据不被重复消费
* client lib完成failover的处理，当消费进程crash、机器宕机的情况下，之前被消费的shard自动迁移至其他活着的进程
* 提供更新check point的接口，供应用方控制check point的持久化频率

### 4.2 限制
* 在网络异常、机器宕机，checkpoint持久化失败的情况下，数据有可能被重复消费
* Load balance只提供到shard个数基本，即，使得各个instance 消费的shard 个数在shard_count/instance_count 左右
* 强依赖外部状态存储系统，如果存储系统异常，可能导致所有worker instance都不再消费数据

## 5. 实现
### 5.1 整体框架
![loghub_client](http://img2.tbcdn.cn/L1/461/1/6303a7de00771fba75827dd95fd842f5bf60afc8)
### 5.2 数据库Schema
#### 5.2.1 保存worker alive信息的表：loghub_client_worker_instance
|列名|类型|说明|
|---|---|---|
|consume_group	|Char(128)|	PK , 对于某一个logstream的消费组|
|logstream_sig	|Char(64)|	PK，用于表示唯一的<project, logstream>|
|worker_instance|	Char(64)|	PK|
|create_time|	DateTime|	Worker instance 创建的时间|

#### 5.2.2 保存shard租赁信息的表 : loghub_client_shard_lease
|列名|类型|说明|
|---|---|---|
|consume_group	|Char(64)|	PK , 对于某一个logstream的消费组|
|logstream_sig|	Char(64)|	PK，用于表示唯一的<project, logstream>|
|shard_id	|Char(64)|	PK|
|lease_id	|Int(20)|	用户租赁shard使用的id，用于原子的test and set 操作，保证任意时刻，只有一个owner能修改lease的值，也就是能抢到该shard |
|lease_owner|	Char(64)|	当前抢shard lease的owner|
|consumer_owner|	Char(64)|	当前正在消费该shard的owner|
|check_point	|Text|	保存该shard已经被消费到的check point|
|update_time	|DateTime|	只是用于记录更新时间，供监控使用|


### 5.3 租赁协议
* 所有shard的当前信息对所有worker instance可见，worker通过观察update_time， owner的信息，确认当前活着的instance_count
* Worker instance 通过list shard api，获取当前logstream的shard信息
* Worker instance通过租赁无人占用的shard，或者抢占其他人正在使用的shard，使得自己消费的shard个数达到 shard_count/instance_count + (0,1)

#### 5.3.1 worker instance启动
* 将worker instance name注册到loghub_client_worker_instance表
* List shard获取当前shard个数
* 从数据库获取所有shard信息，包括：
    * 每个lease对应的shardid
	* 每个lease当前的owner
* 对于数据库中不存在的shard，创建一个尚未被任何人占用的lease， lease_id为0，lease_owner为空
* 如果 Instance name和lease中的lease_owner相同，则续租该lease
* 创建两个线程：
    * 抢占线程 : 通过占有timeout out的lease，或者抢占其他instance已经占有的lease，来达到平衡，执行时间为lease_timeout_interval * 2
    * 续租线程 : 对于已经hold lease的shard进行续租，定期执行，执行时间间隔小于lease_timeout_interval/2

#### 5.3.2 抢占线程
* 统计以下信息：
    * Live_instance_count ：活跃的instance个数（第一次启动从两个数据库表都需要获取，之后只从loghub_client_shard_lease表获取）
        * 首期启动 ：live_instance = distinct( worker_instance (wher update_time > (now() – 60))  +  lease_owner)
        * 非首次启动 : live_instance = distinct(lease_owner (where not timeout) )
    * shard_count : 所有的shard个数
    * held_shard_count : 已经占用的shard个数
* 计算每个instance应该hold的最多shard个数：
    * To_hold_shard_count = ceil(shard_count/live_instance_count)  // 向上取整
    * 计算需要抢占的shard个数：
        * To_take_shard_count = To_hold_shard_count – held_shard_count
* 如果To_take_shard_count > 0 , 选取需要抢占的shard lease
    * 首先从lease timeout的shard中选择To_take_shard_coun个shard
    * 如果不够，则需要从其他instance抢占shard，抢占完成之后，各个instance处理的最多shard数和最少的shard数不相差不超过1，抢占逻辑如下：
		* 按照instance占用的shard个数从多到少排序
		* 选择前K个instance，使得前K个instance被抢占多个shard之后剩下shard平均数AVG_LEFT大于第K+1个instance当前占用的shard
		* 对于前K个instance，每个抢占合适的shard数，使得该instance剩余的shard和AVG_LEFT相差不超过1
    * 对于timeout的shard，抢占时候，更新lease_owner，consumer_owner为当前instance_name。并设定可消费时间当前时刻。
    * 对于从其他instance抢占的shard，只更新lease_owner为intancea_name，不更新consumer_owner。设定可消费时间为当前时刻 + lease_timeout_interval。 // 需要等待被抢占者因为lease过期退出之后才能消费
* 对于抢占成功的shard，加入到续租线程

#### 5.3.3 续租线程
* 对于已经hold的lease，进行续租：
	* 如果系统时间大于shard可消费时间，则更新lease_id为lease_id + 1, consumer_owner为instance_name
	* 否则只更新lease_id为lease_id + 1

#### 5.3.4 lease timeout 判断
每个instance worker以自己保存的内存时间判断一个lease是否timeout。
* Worker从数据库中，list所有shard的lease的时候，判断一个shard的lease是否第一次看到，如果是，则将lease的last_update_time（instance认为lease更新时间）设置成当前系统时间
* 如果lease的lease_id为0， last_update_time 设置成0
* 如果lease_id和上次看到的不一样， last_update_time同样设置成系统时间
* 如果lease_id和上次看到相同，则last_update_time不变（即上次看到lease时候设置的时间）
* 如果sys_time – last_update_time > lease_timeout_interval, 则instance任务该lease已经超时
* 如果instance去抢一个lease超时的shard，则使用当前超时shard的lease_id去竞争，如果抢成功，表示在lease_timeout_interval内，没有其他人更新过该shard的lease

###  5.4 CheckPoint
LogHub client提供check point相关的接口完成check point的操作。Checkpoint的内容由loghub的cursor信息。
#### 5.4.1 worker instance初始化check point
* 当一个shard被确定可消费的时候，client自动从数据库load check point
* 如果数据库没有，则根据配置，确定是从shard的begin或者end开始读取数据
#### 5.4.2 worker instance持久化check point
* Client提供一下接口进行check point的操作：
    * saveCheckPoint (Bool persistent) // 保存check point到内存中或外部系统，如果persistent为true则放到外部持久化系统，否则就放在内存中，同时，后台会定期将长时间没有持久化的check point保存到外部系统
* 当一个shard被其他instance抢占之后，LogHub client会将用户上次内存中save的check point持久化到数据库
* 只有当数据库中， consumer_onwer 和 instance_name相同的时候，才能持久化check point（这个时候，lease可能已经被其他instance抢占了）

### 5.5 执行主逻辑
执行框架是一个每隔一定时间执行一次的循环，执行以下逻辑：
* 获取当前hold lease的shard
* 为这些shard生成一个consumer（如果没有）
* consumer内部是一个状态机，有几种状态：INITIALIZING, PROCESSING,  STOPPING, STOP_COMPLETED
* 在每一种状态的时候，都会生成一个task来完成，task会提交到并发线程池中执行
* 框架每次会调用consumer的run函数，执行：
    * 检测上次的task是否执行成功
    * 状态转换
    * 提交新的task
* 在STOPPING的过程中，会将该shard的check point信息持久化

## 6. 用户API
### 6.1 ILogHubProcessor 
用户需要实现的主要接口，负责数据的处理
```
public void initialize(String shardId) // 当一个shard分配给一个worker instance之后，执行初始化操作
public void process(List<LogGroup> logGroups, ILogHubCheckPointTracker checkPointTracker) // 用户处理数据的接口
public void shutdown(ILogHubCheckPointTracker checkPointTracker) // 当一个shard被抢占之后，会调用该接口完成结束操作
```
### 6.2 ILogHubProcessorFactory
用户用于生成实现ILogHubProcessor接口实例的工厂类
```
public ILogHubProcessor generatorProcessor() // 生成一个LogHubProcessor对象实例
```

## 7. 和其他类型系统对比
Kafka、kinesis提供类似的数据临时存储，实时消费供，同样也会有类似问题。 Kafka的client是直接和Kafka集群内zookeeper连接，来完成client之间的同步，而kinesis client lib使用用户配置的dynamodb完成同步。

**数据重复消费**：kafka和kinesis client在新增worker instance进行rebalance的时候，会出现数据重复消费的情况（被抢占者不能将最新check point信息持久化）。LogHub client在shard租赁协议中，将lease owner和consumer owner做了分离，新的worker instance在抢占shard的lease owner之后，会等待一段时间之后才去更新shard的consumer owner，以确保被抢占的instance worker有足够的时间来更新其最后消费位置的check point，使得抢占者在设置consumer owner之后，可从正确的位置继续消费而不引入重复数据。 

**负载均衡收敛速度**：在shard个数、instance worker个数发生变化的时候， LogHub client lib会批量抢占其他instance worker占用的shard，相对于Kinesis在每轮抢占只抢占1个shard，能使整个系统能尽快达到负载均衡状态。


# LogHub Client同步方案
## 1. 问题
LogHub的消费者，绝大部分情况下，都是在分布式环境下进行的，当多个消费实例需要共同消费LogHub中一个logstream数据的时候，消费实际之间的同步会比较困难，特别是存在消费实例出现fail over，Logstream shard个数变化，增加/减少消费实例等情况。自动的load balance，failover处理，如果直接交给LogHub的应用方的话，将变得很困难.

## 2. 目标
LogHub提供一个client，client内部完成自动的load balance，fail over处理。 应用方只需要focuse在数据处理上

## 3. 方案
* Client实现shard 租赁协议
* Client依赖一个外部系保存租赁状态
* Client根据通过计算当前shard个数、instance worker个数、租赁状态信息，通过续租、抢占shard的方式，来达到load balance
* 提供一个数据处理接口，应用层实现内部逻辑，以及触发check point

## 4. 功能 & 限制
### 4.1 功能
* 支持LogStream shard个数动态变化 
* 支持worker instance个数动态变化，完成负载动态迁移，可保证数据不被重复消费
* 提供更新check point的接口，供应用方控制check point的持久化

### 4.2 限制
* 在网络异常、机器宕机，checkpoint持久化失败的情况下，数据有可能被重复消费
* Load balance只提供到shard个数基本，即，使得各个instance 消费的shard 个数在shard_count/instance_count 左右
* 强依赖外部状态存储系统，如果存储系统异常，可能导致所有worker instance都不再消费数据

## 5. 实现
### 5.1 整体框架
### 5.2 数据库Schema
#### 5.2.1 保存worker alive信息的表：loghub_client_worker_instance
#### 5.2.2 保存shard租赁信息的表 : loghub_client_shard_lease

### 5.3 租赁协议
* 所有shard的当前信息对所有worker instance可见，worker通过观察update_time， owner的信息，确认当前活着的instance_count
* Worker instance 通过list shard api，获取当前logstream的shard_count
* Worker instance通过租赁无人占用的shard，或者抢占其他人正在使用的shard，使得自己消费的shard个数达到 shard_count/instance_count + (0,1)

#### 5.3.1 worker instance启动
* 将worker instance name注册到loghub_client_worker_instance表
* List shard获取当前shard个数
* 从数据库获取所有shard信息，包括：
    * Lease没有超时的shard
    * Lease超时的shard（一定时间没有更新）
* 对于数据库中不存在的shard，创建一个尚未被任何人占用的lease， lease_id为0
* 如果 Instance name和lease中的lease_owner相同，则续租该lease
* 创建两个线程：
    * 抢占线程 : 通过占有timeout out的lease，或者抢占其他instance已经占有的lease，来达到平衡，执行时间为lease_timeout_interval * 2
    * 续租线程 : 对于已经hold lease的shard进行续租，定期执行，执行时间间隔小于lease_timeout_interval/2

#### 5.3.2 强占线程
||||
|---|---|----|
||||
|||








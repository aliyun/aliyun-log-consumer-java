# LogHub Client同步方案
## 1. 问题
LogHub的消费者，绝大部分情况下，都是在分布式环境下进行的，当多个消费实例需要共同消费LogHub中一个logstream数据的时候，消费实际之间的同步会比较困难，特别是存在消费实例出现fail over，Logstream shard个数变化，增加/减少消费实例等情况。自动的load balance，failover处理，如果直接交给LogHub的应用方的话，将变得很困难.
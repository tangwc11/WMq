## 一、WMq是什么？

WMq是MQ消息中间件的一个最小实现，设计思想借鉴了Kafka+RocketMQ，本实现仅用作个人学习和交流，用于加深自己对于消息中间件系统的理解，**不建议用作生产环境 : )**

### 1. 核心角色

类似于Kafka，WMq有以下几大角色：

- **Zookeeper**：使用Zookeeper作为注册中心和协调中心，利用其CP特性实现节点状态统一，以及提供的watch机制触发集群状态变更
- **Broker**：核心角色，几大核心功能：对接Producer负责消息写入和存储，对接Consumer负责消息的读取。
- **Client**：有两个角色Producer和Consumer
  - **Producer**：消息生产者，通过Zookeeper连接Broker进行消息写入
  - **Consumer**（Group）：消息的消费者，通过Zookeeper连接Broker进行消息读取，使用**Pull模式**拉取消息
- **Admin**：提供基本的Dashboard和Topic的增（已实现）删改查，实际是对Zookeeper节点的增删改查



### 2. 核心概念

同样类似于Kafka，WMq定义了以下核心概念：

- **Cluster**：集群，用于环境隔离，体现在Zookeeper上就是`/wmq/clusterName`这个二级目录
- **Topic**：消息的主题，用于区分消息
- **Partition**：消息的分区，用于提升写入性能和消费性能，将同一个Topic的消息分散为多个Partition分区，在RocketMQ中是Queue的概念
- **Key**：一个消息，默认被随机分配给不同的Partition，如果想指定分配规则，可以使用Key+自定义PartitionRouter实现（一般用作顺序消费）
- **Uniq**：独创的概念，类似于RocketMQ的key，用作消息去重（基于短时间的时间窗口），比如10s内，只允许第一条带有相同Uniq的消息被发送
- **Replica**：消息的副本，用于提升高可用，在Broker中副本Follower和Leader错开部署，用于提升数据安全
- **Leader-Broker**：Leader-Follower是对应Partition而言的，一个Broker真多TopicA-Partition1可能是Leader，针对Partition2可能是Follower
- **Follower-Broker**：Follower不对Client服务，只是用于同步Leader的数据，并作为Leader的候选者
- **ISR**：为了保证消息的安全，不能随便选取Follower作为Leader，而是应该选举具备最高同步的Follower作为候选者，ISR就是这个最高同步着的集合
- **Offset**：偏移量，对于同一个Topic-Partition的消息，offset代表改消息在所有dbfile中的总偏移量
- **CommitLog**：原始日志文件，存在于在Broker宿主机的磁盘中
  - 文件目录格式：按/wmq/db/topic/partition/file的层次目录进行存储
  - 文件命名格式：第一个文件总是以`000000000.wmqdb`命名，9位命名支持的最大单个文件的大小是GB级别，第二个文件名是第一个大小，第三个文件是前两个文件的大小只和，依次类推
  - 用以上命名方式的好处在于，通过一个全局的offset，就能定位到是哪个文件（读和写皆可），提升了便利程度
- **Consumer-Group**：抽象出的Consumer往上的一层，用于更好的扩展Consumer消费，同一个Consumer-Group中可以有多个Consumer，这些Consumer分担所有的Partition。



最终的角色架构关系图如下：

![wmq.drawio](imgs/wmq.drawio-4427415.svg)





## 二、QuickStart

1. git clone项目

2. 执行`mvn clean package -U`打包项目

3. 在任一目录下执行java -jar 命令，运行不同的角色（**该目录将作为日志和DB文件的根目录**）

4. 启动顺序及命令建议如下：

   0. Zookeeper

   1. Broker：
   2. Admin：
   3. Producer：
   4. Consumer：

5. 



## 三、核心原理

设计理念大部分借鉴于Kafka，关于Kafka的详细讲解可以参考[博文](https://www.cnblogs.com/makai/p/12720424.html)

### 1. Zookeeper目录结构



```
WMq中zookeeper目录结构
/wmq
  |---clusterName：集群根目录
        |---topic-registry：Topic注册根目录

        |---broker-id-registry：broker的信息，被broker和client监听，动态的更改topic-partition-registry
                |---broker-1：host,port：临时节点，断联自动消失，可以监听
                |---broker-2：host,port
                |---broker-3：host,port
        |---topic-partition-registry：分区信息，leader follower信息，节点修改时，会推送到各个brokerState，以及consumer
                |---topicA：leader，partition，followers
                |---topicB：leader，partition，followers
                |---topicC：leader，partition，followers
        |---consumer-offset：消费进度
                |---topicA_group_partition：offset
        |---consumer-id-registry
                |---consumer-group:
                          
                          
                          
                          
public static final String BASE = "/wmq";

    public static final String TOPIC_REGISTRY = "topic-registry";
    public static final String TOPIC_REGISTRY_SIGNAL = "topic-registry-signal";

    public static final String BROKER_REGISTRY = "broker-registry";
    public static final String PARTITION_REGISTRY = "partition-registry";
    public static final String PARTITION_REGISTRY_VERSION = "partition-registry-version";

    public static final String CLIENT_REGISTRY = "client-registry";//永久目录

    public static final String CONSUMER_OFFSET_DIR = "consumer-offset";//永久目录
    public static final String CONSUMER_OFFSET_NODE = "[%s|%s]";//${group}-${partition}";//消费进度 永久节点

    public static final String CONSUMER_GROUP = "consumer-group-registry";//消费组关系，永久节点
    public static final String CONSUMER_GROUP_INST_NODE = "[%s|%s|%s]";//${topic}-${group}-${partition}";//临时节点

```







在编写过程中，有很多地方需要考虑，针对性能提升，可以从以下节点进行考虑：

1. 序列化
2. NIO
3. 零拷贝（kafka使用的sendfile，rocketmq使用的mmap，总的来说sendfile性能更高也就导致kafka的吞吐量更好，rocketmq的功能更多）
4. 批量写、批量读（与一致性有点冲突，需要权衡）
5. 对象的精简、共用（考验编程工程化的功底）
6. 数据结构和算法（数据量大的时候，低效算法耗时指数级上升）







使用protobuff作为序列化框架，本机测试

简单测试写

```bash
cost:1174783789 ns
average:11747 ns
qps:85122.05
```

简单测试读

```bash
read total:100000 msg 
cost:621079028 ns
average:6210 ns
qps:161010.11
```

10万条消息，dbfile文件大小约为1.4MB






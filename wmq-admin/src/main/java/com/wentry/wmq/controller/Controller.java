package com.wentry.wmq.controller;

import com.wentry.wmq.domain.AdminState;
import com.wentry.wmq.domain.dashboard.ClusterSnapshot;
import com.wentry.wmq.domain.dashboard.TopicBaseInfo;
import com.wentry.wmq.domain.dashboard.TopicConsumerInfo;
import com.wentry.wmq.domain.dashboard.TopicDetail;
import com.wentry.wmq.domain.dashboard.TopicBrokerInfo;
import com.wentry.wmq.domain.registry.clients.ConsumerInstanceInfo;
import com.wentry.wmq.domain.registry.offset.OffsetInfo;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.topic.TopicInfo;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;
import com.wentry.wmq.utils.json.JsonUtils;
import com.wentry.wmq.utils.zk.ZkPaths;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 */
@RestController
@RequestMapping("/wmq/admin")
public class Controller {

    @Autowired
    AdminState adminState;

    @Autowired
    ZkRegistry registry;

    @RequestMapping("/changeCluster")
    public String changeCluster(String cluster) {
        adminState.setClusterName(cluster);
        return "ok";
    }

    @RequestMapping("/topic/create")
    public String createTopic(String topic,
                              @RequestParam(required = false, defaultValue = "1") int partition,
                              @RequestParam(required = false, defaultValue = "1") int replica) {

        if (partition <= 0 || replica < 0) {
            return "入参错误";
        }

        Stat stat = registry.checkExist(ZkPaths.getTopicRegistryNode(adminState.getClusterName(), topic));

        if (stat != null) {
            return "topic已存在";
        }

        boolean res = registry.updateVal(
                CreateMode.PERSISTENT,
                ZkPaths.getTopicRegistryNode(adminState.getClusterName(), topic),
                new TopicInfo().setTopic(topic).setPartition(partition).setReplica(replica)
        );
        if (!res) {
            return "未知错误";
        }

        TopicInfo topicInfo = new TopicInfo().setTopic(topic).setPartition(partition).setReplica(replica);

        //创建partition，此时leader和follower是空的，会触发broker任何一个节点进行计算并广播
        PartitionInfo partitionInfo = new PartitionInfo().setTopic(topicInfo.getTopic()).setPartitionTotal(topicInfo.getPartition());

        boolean partitionAddRes = registry.updateVal(CreateMode.PERSISTENT,
                ZkPaths.getPartitionRegistryChildPath(adminState.getClusterName(), topic),
                partitionInfo);

        if (!partitionAddRes) {
            return "添加partitionInfo失败";
        }

        registry.updateVal(CreateMode.PERSISTENT,
                ZkPaths.getTopicRegistrySignal(adminState.getClusterName()),
                System.currentTimeMillis());

        return "ok";
    }

    @RequestMapping("/dashboard")
    public String dashboard(@RequestParam(required = false) String topic) {
        ClusterSnapshot snapshot = new ClusterSnapshot();

        snapshot.setClusterName(adminState.getClusterName());

        List<TopicInfo> topics = new ArrayList<>();
        if (StringUtils.isNotBlank(topic)) {
            TopicInfo topicInfo = registry.getPathValue(
                    ZkPaths.getTopicRegistryNode(adminState.getClusterName(), topic), TopicInfo.class
            );
            if (topicInfo == null) {
                return "none info for topic:" + topic;
            }
            topics.add(topicInfo);
        } else {
            //获取所有topic列表
            topics = registry.getPathChildrenValues(
                    ZkPaths.getTopicRegistryDir(adminState.getClusterName()), TopicInfo.class
            );
        }

        snapshot.setTopics(topics);
        Map<String, TopicDetail> topicDetails = snapshot.getTopicDetails();
        for (TopicInfo topicInfo : topics) {
            TopicDetail topicDetail = new TopicDetail().setBaseInfo(new TopicBaseInfo()
                    .setName(topicInfo.getTopic())
                    .setPartition(topicInfo.getPartition())
                    .setReplica(topicInfo.getReplica())
            );
            topicDetails.put(topicInfo.getTopic(), topicDetail);
            fillDetail(topicDetail);
        }

        return JsonUtils.toJson(snapshot);
    }

    private void fillDetail(TopicDetail topicDetail) {
        TopicBaseInfo topic = topicDetail.getBaseInfo();

        //先有分区，再有broker，再有producer和consumer
        PartitionInfo partition = registry.getPathValue(
                ZkPaths.getPartitionRegistryChildPath(adminState.getClusterName(), topic.getName()), PartitionInfo.class
        );

        if (partition == null) {
            return;
        }

        Map<Integer, String> partitionLeader = partition.getPartitionLeader();
        Map<Integer, Set<String>> partitionFollowers = partition.getPartitionFollowers();
        for (int i = 0; i < topic.getPartition(); i++) {
            topicDetail.getBrokers().add(
                    new TopicBrokerInfo()
                            .setPartition(i)
                            .setLeader(partitionLeader.get(i))
                            .setFollowers(partitionFollowers.get(i))
            );
        }

        List<OffsetInfo> offsetsInfos = registry.getPathChildrenValues(
                ZkPaths.getConsumerOffsetDir(adminState.getClusterName(), topic.getName()), OffsetInfo.class
        );

        Map<String, List<TopicConsumerInfo>> groupConsumers = topicDetail.getGroupConsumers();

        Map<String, List<OffsetInfo>> groupOffsets = offsetsInfos.stream().collect(Collectors.groupingBy(OffsetInfo::getGroup));
        for (Map.Entry<String, List<OffsetInfo>> eachGroupOffsets : groupOffsets.entrySet()) {
            String group = eachGroupOffsets.getKey();
            List<OffsetInfo> offsets = eachGroupOffsets.getValue();
            Map<Integer, OffsetInfo> partitionOffset = offsets.stream().collect(Collectors.toMap(OffsetInfo::getPartition, x -> x, (o, n) -> n));
            for (int parition = 0; parition < topic.getPartition(); parition++) {
                OffsetInfo offsetInfo = partitionOffset.get(parition);

                TopicConsumerInfo consumerInfo;
                if (offsetInfo == null) {
                    consumerInfo = new TopicConsumerInfo().setPartition(parition).setMsg("未获取到offset信息");
                } else {
                    consumerInfo = new TopicConsumerInfo()
                            .setPartition(parition)
                            .setGroup(group)
                            .setOffset(offsetInfo.getOffset())
                            .setConsumerInstance(getConsumerInstance(topic.getName(), group, parition));
                }
                groupConsumers.computeIfAbsent(group, s -> new ArrayList<>())
                        .add(consumerInfo);
            }
        }

    }

    private String getConsumerInstance(String topic, String group, int partition) {
        ConsumerInstanceInfo consumerInstance = registry.getPathValue(
                ZkPaths.getConsumerInstanceNode(adminState.getClusterName(), topic, group, partition),
                ConsumerInstanceInfo.class
        );
        if (consumerInstance == null) {
            return "未获取到消费实例信息";
        }

        return consumerInstance.getConsumerId() + ":" + consumerInstance.getPort();
    }


}

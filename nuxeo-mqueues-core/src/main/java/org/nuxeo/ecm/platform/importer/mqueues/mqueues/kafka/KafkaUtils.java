/*
 * (C) Copyright 2017 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     bdelbosc
 */
package org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.requests.MetadataResponse;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Misc Kafka Utils
 * @since 9.2
 */
public class KafkaUtils implements AutoCloseable {
    private static final Log log = LogFactory.getLog(KafkaUtils.class);
    private final ZkClient zkClient;
    private final ZkUtils zkUtils;
    public static final String DEFAULT_ZK_SERVER = "localhost:2181";
    public static final int ZK_TIMEOUT_MS = 6000;
    public static final int ZK_CONNECTION_TIMEOUT_MS = 10000;

    public KafkaUtils() {
        this(DEFAULT_ZK_SERVER);
    }

    public KafkaUtils(String zkServers) {
        log.debug("Init zkServers: " + zkServers);
        this.zkClient = createZkClient(zkServers);
        this.zkUtils = createZkUtils(zkServers, zkClient);
    }

    public static boolean kafkaDetected() {
        return kafkaDetected(DEFAULT_ZK_SERVER);
    }

    public static boolean kafkaDetected(String zkServers) {
        try {
            ZkClient tmp = new ZkClient(zkServers, 1000, 1000, ZKStringSerializer$.MODULE$);
            tmp.close();
        } catch (ZkTimeoutException e) {
            return false;
        }
        return true;
    }

    private static ZkUtils createZkUtils(String zkServers, ZkClient zkClient) {
        return new ZkUtils(zkClient, new ZkConnection(zkServers), false);
    }

    private static ZkClient createZkClient(String zkServers) {
        return new ZkClient(zkServers, ZK_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, ZKStringSerializer$.MODULE$);
    }

    public void createTopic(String topic, int partitions) {
        createTopic(topic, partitions, 1);
    }

    public void createTopic(String topic, int partitions, int replicationFactor) {
        log.info("Creating topic: " + topic + ", partitions: " + partitions + ", replications: " + replicationFactor);
        if (AdminUtils.topicExists(zkUtils, topic)) {
            String msg = "Can not create Topic already exists: " + topic;
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor,
                new Properties(), RackAwareMode.Disabled$.MODULE$);
    }

    public boolean topicExists(String topic) {
        return AdminUtils.topicExists(zkUtils, topic);
    }

    /**
     * Work only if delete.topic.enable is true which is not the default
     */
    public void markTopicForDeletion(String topic) {
        log.debug("mark topic for deletion: " + topic);
        AdminUtils.deleteTopic(zkUtils, topic);
    }

    public int getNumberOfPartitions(String topic) {
        MetadataResponse.TopicMetadata metadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
        return metadata.partitionMetadata().size();
    }

    public void resetConsumerStates(String topic) {
        log.debug("Resetting consumer states");
        AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic);
    }

    public Set<String> getBrokerEndPoints() {
        Set<String> ret = new HashSet<>();
        Seq<Broker> brokers = zkUtils.getAllBrokersInCluster();
        Broker broker;
        Iterator<Broker> iter = brokers.iterator();
        while (iter.hasNext()) {
            broker = iter.next();
            if (broker != null) {
                Seq<EndPoint> endPoints = broker.endPoints();
                Iterator<EndPoint> iter2 = endPoints.iterator();
                while (iter2.hasNext()) {
                    EndPoint endPoint = iter2.next();
                    ret.add(endPoint.connectionString());
                }
            }
        }
        return ret;
    }

    public String getDefaultBootstrapServers() {
        return getBrokerEndPoints().stream().collect(Collectors.joining(","));
    }

    @Override
    public void close() throws Exception {
        if (zkUtils != null) {
            zkUtils.close();
        }
        if (zkClient != null) {
            zkClient.close();
        }
        log.debug("Closed.");
    }


    public static List<List<MQPartition>> rangeAssignments(int threads, Map<String, Integer> streams) {
        PartitionAssignor assignor = new RangeAssignor();
        return assignments(assignor, threads, streams);
    }

    public static List<List<MQPartition>> roundRobinAssignments(int threads, Map<String, Integer> streams) {
        PartitionAssignor assignor = new RoundRobinAssignor();
        return assignments(assignor, threads, streams);
    }


    protected static List<List<MQPartition>> assignments(PartitionAssignor assignor, int threads, Map<String, Integer> streams) {
        final List<PartitionInfo> parts = new ArrayList<>();
        streams.forEach((streamName, size) -> parts.addAll(getPartsFor(streamName, size)));
        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>();
        List<String> streamNames = streams.keySet().stream().sorted().collect(Collectors.toList());
        for (int i = 0; i < threads; i++) {
            subscriptions.put(String.valueOf(i), new PartitionAssignor.Subscription(streamNames));
        }
        Cluster cluster = new Cluster("kafka-cluster", Collections.emptyList(), parts,
                Collections.<String>emptySet(), Collections.<String>emptySet());
        Map<String, PartitionAssignor.Assignment> assignments = assignor.assign(cluster, subscriptions);
        List<List<MQPartition>> ret = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            ret.add(assignments.get(String.valueOf(i)).partitions().stream()
                    .map(part -> new MQPartition(part.topic(), part.partition()))
                    .collect(Collectors.toList()));
        }
        return ret;
    }

    protected static Collection<PartitionInfo> getPartsFor(String topic, int partitions) {
        Collection<PartitionInfo> ret = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            ret.add(new PartitionInfo(topic, i, null, null, null));
        }
        return ret;
    }

}

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
package org.nuxeo.lib.core.mqueues.mqueues.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.nuxeo.lib.core.mqueues.mqueues.MQAppender;
import org.nuxeo.lib.core.mqueues.mqueues.MQLag;
import org.nuxeo.lib.core.mqueues.mqueues.MQPartition;
import org.nuxeo.lib.core.mqueues.mqueues.MQRebalanceListener;
import org.nuxeo.lib.core.mqueues.mqueues.MQTailer;
import org.nuxeo.lib.core.mqueues.mqueues.internals.AbstractMQManager;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @since 9.2
 */
public class KafkaMQManager extends AbstractMQManager {
    public static final String DISABLE_SUBSCRIBE_PROP = "subscribe.disable";
    public static final String DEFAULT_REPLICATION_FACTOR_PROP = "default.replication.factor";

    protected final KafkaUtils kUtils;
    protected final Properties producerProperties;
    protected final Properties consumerProperties;
    protected final Properties adminProperties;
    protected final String prefix;
    protected final short defaultReplicationFactor;
    protected boolean disableSubscribe = false;


    public KafkaMQManager(String zkServers, Properties producerProperties, Properties consumerProperties) {
        this(zkServers, null, producerProperties, consumerProperties);
    }

    public KafkaMQManager(String zkServers, String topicPrefix, Properties producerProperties, Properties consumerProperties) {
        this.prefix = (topicPrefix != null) ? topicPrefix : "";
        this.kUtils = new KafkaUtils(zkServers);
        disableSubscribe = Boolean.valueOf(consumerProperties.getProperty(DISABLE_SUBSCRIBE_PROP, "false"));
        defaultReplicationFactor = Short.parseShort(producerProperties.getProperty(DEFAULT_REPLICATION_FACTOR_PROP, "1"));
        this.producerProperties = normalizeProducerProperties(producerProperties);
        this.consumerProperties = normalizeConsumerProperties(consumerProperties);
        this.adminProperties = createAdminProperties(producerProperties, consumerProperties);
    }

    protected String getTopicName(String name) {
        return prefix + name;
    }

    protected String getNameFromTopic(String topic) {
        if (!topic.startsWith(prefix)) {
            throw new IllegalArgumentException(String.format("topic %s with invalid prefix %s", topic, prefix));
        }
        return topic.replaceFirst(prefix, "");
    }

    @Override
    public void create(String name, int size) {
        kUtils.createTopic(getAdminProperties(), getTopicName(name), size, defaultReplicationFactor);
    }

    @Override
    public boolean exists(String name) {
        return kUtils.topicExists(getTopicName(name));
    }


    @Override
    public <M extends Externalizable> MQAppender<M> createAppender(String name) {
        return KafkaMQAppender.open(getTopicName(name), name, producerProperties, consumerProperties);
    }

    @Override
    protected <M extends Externalizable> MQTailer<M> acquireTailer(Collection<MQPartition> partitions, String group) {
        partitions.forEach(this::checkValidPartition);
        return KafkaMQTailer.createAndAssign(prefix, partitions, group, (Properties) consumerProperties.clone());
    }

    protected void checkValidPartition(MQPartition partition) {
        int partitions = kUtils.getNumberOfPartitions(getAdminProperties(), getTopicName(partition.name()));
        if (partition.partition() >= partitions) {
            throw new IllegalArgumentException("Partition out of bound " + partition + " max: " + partitions);
        }
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public Properties getConsumerProperties() {
        return consumerProperties;
    }

    public Properties getAdminProperties() {
        return adminProperties;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (kUtils != null) {
            kUtils.close();
        }
    }

    @Override
    public boolean supportSubscribe() {
        return !disableSubscribe;
    }

    @Override
    protected <M extends Externalizable> MQTailer<M> doSubscribe(String group, Collection<String> names, MQRebalanceListener listener) {
        return KafkaMQTailer.createAndSubscribe(prefix, names, group, (Properties) consumerProperties.clone(), listener);
    }

    protected static Properties normalizeConsumerProperties(Properties consumerProperties) {
        Properties ret;
        if (consumerProperties != null) {
            ret = (Properties) consumerProperties.clone();
        } else {
            ret = new Properties();
        }
        ret.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        ret.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
        ret.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        ret.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ret.remove(DISABLE_SUBSCRIBE_PROP);
        return ret;
    }

    protected Properties normalizeProducerProperties(Properties producerProperties) {
        Properties ret;
        if (producerProperties != null) {
            ret = (Properties) producerProperties.clone();
        } else {
            ret = new Properties();
        }
        ret.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        ret.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesSerializer");
        ret.remove(DEFAULT_REPLICATION_FACTOR_PROP);
        return ret;
    }

    protected Properties createAdminProperties(Properties producerProperties, Properties consumerProperties) {
        Properties ret = new Properties();
        ret.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                producerProperties.getOrDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        consumerProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)));
        return ret;
    }

    @Override
    public List<MQLag> getLagPerPartition(String name, String group) {
        MQLag[] ret;
        Properties props = (Properties) consumerProperties.clone();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        try (KafkaConsumer<String, Bytes> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> topicPartitions = new ArrayList<>();
            consumer.partitionsFor(getTopicName(name)).forEach(
                    meta -> topicPartitions.add(new TopicPartition(meta.topic(), meta.partition())));
            ret = new MQLag[topicPartitions.size()];
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            for (TopicPartition topicPartition : topicPartitions) {
                long committedOffset = 0L;
                OffsetAndMetadata committed = consumer.committed(topicPartition);
                if (committed != null) {
                    committedOffset = committed.offset();
                }
                Long endOffset = endOffsets.get(topicPartition);
                if (endOffset == null) {
                    endOffset = 0L;
                }
                ret[topicPartition.partition()] = new MQLag(committedOffset, endOffset);
            }
        }
        return Arrays.asList(ret);
    }

    @Override
    public List<String> listAll() {
        return kUtils.listTopics().stream().filter(name -> name.startsWith(prefix))
                .map(this::getNameFromTopic).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "KafkaMQManager{" +
                "producerProperties=" + producerProperties +
                ", consumerProperties=" + consumerProperties +
                ", prefix='" + prefix + '\'' +
                '}';
    }

    @Override
    public List<String> listConsumerGroups(String name) {
        String topic = getTopicName(name);
        if (!exists(name)) {
            throw new IllegalArgumentException("Unknown MQueue: " + name);
        }
        return kUtils.listConsumers(getProducerProperties(), topic);
    }

}

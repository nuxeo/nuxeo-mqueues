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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.AbstractMQManager;

import java.io.Externalizable;
import java.util.Properties;

/**
 * @since 9.2
 */
public class KafkaMQManager<M extends Externalizable> extends AbstractMQManager<M> {
    private final KafkaUtils kUtils;
    private final Properties producerProperties;
    private final Properties consumerProperties;
    private final String prefix;


    public KafkaMQManager(String zkServers, Properties producerProperties, Properties consumerProperties) {
        this(zkServers, null, producerProperties, consumerProperties);
    }

    public KafkaMQManager(String zkServers, String topicPrefix, Properties producerProperties, Properties consumerProperties) {
        this.prefix = (topicPrefix != null) ? topicPrefix : "";
        this.kUtils = new KafkaUtils(zkServers);
        this.producerProperties = normalizeProducerProperties(producerProperties);
        this.consumerProperties = normalizeConsumerProperties(consumerProperties);
    }

    protected String getTopicName(String name) {
        return prefix + name;
    }

    @Override
    public void create(String name, int size) {
        kUtils.createTopic(getTopicName(name), size);
    }

    @Override
    public boolean exists(String name) {
        return kUtils.topicExists(getTopicName(name));
    }


    @Override
    public MQAppender<M> createAppender(String name) {
        return KafkaMQAppender.open(getTopicName(name), name, producerProperties, consumerProperties);
    }

    @Override
    protected MQTailer<M> acquireTailer(MQPartition partition, String group) {
        int partitions = kUtils.getNumberOfPartitions(getTopicName(partition.name()));
        if (partition.partition() >= partitions) {
            throw new IllegalArgumentException("Partition out of bound " + partition + " max: " + partitions);
        }
        KafkaMQTailer<M> ret = new KafkaMQTailer<>(getTopicName(partition.name()), partition,
                group, (Properties) consumerProperties.clone());
        return ret;
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public Properties getConsumerProperties() {
        return consumerProperties;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (kUtils != null) {
            kUtils.close();
        }
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
        return ret;
    }

}

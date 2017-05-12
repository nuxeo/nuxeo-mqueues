/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 */
package org.nuxeo.ecm.platform.importer.mqueues.tests;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;

import java.time.Duration;
import java.util.Properties;

public class TestMQueuesKafka extends TestMQueues {
    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_PREFIX = "nuxeo-test";
    public final static Duration MIN_DURATION = Duration.ofSeconds(1);
    private String topic;

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Rule
    public TestName testName = new TestName();

    @Override
    public Duration getMinDuration() {
        return MIN_DURATION;
    }

    @Override
    public MQueues<IdMessage> createMQ(int partitions) throws Exception {
        // there is no way to delete a topic so we change topic
        topic = getTopicName(testName.getMethodName());
        try (KafkaUtils kafka = new KafkaUtils()) {
            kafka.createTopic(topic, partitions);
        }
        return KMQueues.open(topic, getProducerProps(), getConsumerProps());
    }

    public static String getTopicName(String name) {
        return TOPIC_PREFIX + "-" + name + "-" + System.currentTimeMillis();
    }

    @Override
    public MQueues<IdMessage> reopenMQ() {
        return KMQueues.open(topic, getProducerProps(), getConsumerProps());
    }

    @Override
    public MQueues<IdMessage> createSameMQ(int partitions) throws Exception {
        try (KafkaUtils kafka = new KafkaUtils()) {
            kafka.createTopic(topic, partitions);
        }
        return KMQueues.open(topic, getProducerProps(), getConsumerProps());
    }

    public static Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        return props;
    }

    public static Properties getConsumerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        return props;
    }
}

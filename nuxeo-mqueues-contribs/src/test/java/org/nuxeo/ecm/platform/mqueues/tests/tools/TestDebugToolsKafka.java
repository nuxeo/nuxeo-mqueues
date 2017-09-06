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
package org.nuxeo.ecm.platform.mqueues.tests.tools;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.lib.core.mqueues.mqueues.kafka.KafkaUtils;

import java.io.Externalizable;
import java.util.Properties;

public class TestDebugToolsKafka extends TestDebugTools {
    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String PREFIX = "nuxeo-";

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Override
    public <M extends Externalizable> MQManager<M> createManager() {
        return new KafkaMQManager<>(KafkaUtils.DEFAULT_ZK_SERVER, PREFIX, getProducerProps(), getConsumerProps());
    }

    public static Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        return props;
    }

    public static Properties getConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        // consumer are removed from a group if there more than this interval between poll
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 20000);
        // session timeout, consumer is removed from a group if there is no heartbeat on this interval
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        // short ht interval so that rebalance don't take for ever
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 400);
        // keep number low to reduce time interval between poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        return props;
    }

}

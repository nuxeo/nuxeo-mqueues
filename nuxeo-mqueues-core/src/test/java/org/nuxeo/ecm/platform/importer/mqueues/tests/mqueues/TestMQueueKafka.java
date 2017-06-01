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
package org.nuxeo.ecm.platform.importer.mqueues.tests.mqueues;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;

import java.time.Duration;
import java.util.Properties;

import static org.jboss.netty.handler.codec.http.multipart.DiskFileUpload.prefix;

public class TestMQueueKafka extends TestMQueue {
    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_PREFIX = "nuxeo-test";
    public final static Duration MIN_DURATION = Duration.ofSeconds(1);
    protected String prefix;

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Override
    public Duration getMinDuration() {
        return MIN_DURATION;
    }

    @Override
    public MQManager<IdMessage> createManager() throws Exception {
        if (prefix == null) {
            prefix = getPrefix();
        }
        return new KafkaMQManager<>(KafkaUtils.DEFAULT_ZK_SERVER, prefix, getProducerProps(), getConsumerProps());
    }

    @After
    public void resetPrefix() {
        prefix = null;
    }

    public static String getPrefix() {
        return TOPIC_PREFIX + "-" + System.currentTimeMillis() + "-";
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

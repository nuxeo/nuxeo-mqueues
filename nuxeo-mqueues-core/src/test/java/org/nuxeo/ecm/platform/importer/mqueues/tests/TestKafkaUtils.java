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
package org.nuxeo.ecm.platform.importer.mqueues.tests;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @since 9.2
 */
public class TestKafkaUtils {
    private static final String DEFAULT_TOPIC = "nuxeo-test-default-topic-10";
    private static final int DEFAULT_TOPIC_PARTITION = 10;

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    protected void createDefaultTopicIfNeeded(KafkaUtils kutils) {
        if (!kutils.topicExists(DEFAULT_TOPIC)) {
            kutils.createTopic(DEFAULT_TOPIC, DEFAULT_TOPIC_PARTITION);
        }
    }

    @Test
    public void testCreateTopic() throws Exception {
        try (KafkaUtils kutils = new KafkaUtils()) {
            createDefaultTopicIfNeeded(kutils);
            assertEquals(DEFAULT_TOPIC_PARTITION, kutils.getNumberOfPartitions(DEFAULT_TOPIC));
        }

    }

    @Test
    public void testResetConusmerStates() throws Exception {
        try (KafkaUtils kutils = new KafkaUtils()) {
            createDefaultTopicIfNeeded(kutils);
            kutils.resetConsumerStates(DEFAULT_TOPIC);
        }
    }

    @Test
    public void testBrokerList() throws Exception {
        try (KafkaUtils kutils = new KafkaUtils()) {
            assertNotNull(kutils.getBrokerEndPoints());
            assertEquals(Collections.singleton("PLAINTEXT://localhost:9092"), kutils.getBrokerEndPoints());
        }
    }

    @Test
    public void testDefaultBootstrapServers() throws Exception {
        try (KafkaUtils kutils = new KafkaUtils()) {
            assertEquals("PLAINTEXT://localhost:9092", kutils.getDefaultBootstrapServers());

        }
    }


}

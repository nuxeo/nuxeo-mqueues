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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;

/**
 * @since 9.2
 */
public class TestPatternQueuingKafka extends TestPatternQueuing {
    private String topic;

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Rule
    public TestName testName = new TestName();

    @Override
    public MQueues<IdMessage> createMQ(int partitions) throws Exception {
        topic = TestMQueuesKafka.getTopicName(testName.getMethodName());
        try (KafkaUtils kafka = new KafkaUtils()) {
            kafka.createTopic(topic, partitions);
        }
        return KMQueues.open(topic, TestMQueuesKafka.getProducerProps(),
                TestMQueuesKafka.getConsumerProps());
    }

    @Override
    public MQueues<IdMessage> reopenMQ() {
        return KMQueues.open(topic, TestMQueuesKafka.getProducerProps(),
                TestMQueuesKafka.getConsumerProps());
    }

    @Override
    @Test
    @Ignore("hang on future.get")
    public void killMQueues() throws Exception {

    }
}

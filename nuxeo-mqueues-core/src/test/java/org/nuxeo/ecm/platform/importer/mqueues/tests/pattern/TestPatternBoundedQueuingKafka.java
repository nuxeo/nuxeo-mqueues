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
package org.nuxeo.ecm.platform.importer.mqueues.tests.pattern;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.tests.mqueues.TestMQueueKafka;

public class TestPatternBoundedQueuingKafka extends TestPatternBoundedQueuing {

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Override
    public MQManager<IdMessage> createManager() throws Exception {
        String prefix = getPrefix(name.getMethodName());
        return new KafkaMQManager<>(KafkaUtils.DEFAULT_ZK_SERVER, prefix,
                TestMQueueKafka.getProducerProps(),
                TestMQueueKafka.getConsumerProps());
    }

    public static String getPrefix(String name) {
        return TestMQueueKafka.TOPIC_PREFIX + "-" + System.currentTimeMillis() + "-";
    }

    @Override
    public int getNbDocumentForBuggyConsumerTest() {
        return 127;
    }
}

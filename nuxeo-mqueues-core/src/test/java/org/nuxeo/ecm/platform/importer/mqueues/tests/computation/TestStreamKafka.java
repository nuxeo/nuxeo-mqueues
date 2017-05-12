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
package org.nuxeo.ecm.platform.importer.mqueues.tests.computation;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Streams;
import org.nuxeo.ecm.platform.importer.mqueues.streams.mqueues.StreamsMQKafka;
import org.nuxeo.ecm.platform.importer.mqueues.tests.TestMQueuesKafka;

import java.io.IOException;

/**
 * @since 9.2
 */
public class TestStreamKafka extends TestStream {

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Rule
    public TestName testName = new TestName();
    private String prefix;

    public String getTopicPrefix(String mark) {
        return "nuxeo-test-" + testName.getMethodName() + "-" + System.currentTimeMillis() + "-";
    }

    @Override
    public Streams getStreams() throws Exception {
        this.prefix = getTopicPrefix(testName.getMethodName());
        return new StreamsMQKafka(KafkaUtils.DEFAULT_ZK_SERVER, prefix,
                TestMQueuesKafka.getProducerProps(),
                TestMQueuesKafka.getConsumerProps());
    }

    @Override
    public Streams getSameStreams() throws IOException {
        return new StreamsMQKafka(KafkaUtils.DEFAULT_ZK_SERVER, prefix,
                TestMQueuesKafka.getProducerProps(),
                TestMQueuesKafka.getConsumerProps());
    }

}

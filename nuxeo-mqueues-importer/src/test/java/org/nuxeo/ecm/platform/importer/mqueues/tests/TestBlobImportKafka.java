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
import org.nuxeo.ecm.platform.importer.mqueues.kafka.KafkaConfigService;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.lib.core.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.LocalDeploy;

/**
 * @since 9.2
 */
@Deploy({"org.nuxeo.ecm.mqueues.importer"})
@LocalDeploy("org.nuxeo.ecm.mqueues.kafka.tests.contrib:test-kafka-config-contrib.xml")
public class TestBlobImportKafka extends TestBlobImport {

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Override
    public MQManager getManager() throws Exception {
        KafkaConfigService service = Framework.getService(KafkaConfigService.class);
        String kafkaConfig = "default";
        return new KafkaMQManager<>(service.getZkServers(kafkaConfig),
                service.getTopicPrefix(kafkaConfig),
                service.getProducerProperties(kafkaConfig),
                service.getConsumerProperties(kafkaConfig));
    }
}

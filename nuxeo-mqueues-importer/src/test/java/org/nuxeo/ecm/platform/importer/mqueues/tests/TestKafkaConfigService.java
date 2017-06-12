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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.importer.mqueues.kafka.KafkaConfigService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @since 9.2
 */
@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy({"org.nuxeo.ecm.mqueues.importer", "org.nuxeo.ecm.automation.core", "org.nuxeo.ecm.core.io"})
@LocalDeploy("org.nuxeo.ecm.mqueues.kafka.tests.contrib:test-kafka-config-contrib.xml")
public class TestKafkaConfigService {

    @Test
    public void testService() {
        KafkaConfigService service = Framework.getService(KafkaConfigService.class);
        assertNotNull(service);
        assertFalse(service.getConfigNames().isEmpty());
        assertEquals(3, service.getConfigNames().size());

        String config1 = "default";
        assertEquals("localhost:2181", service.getZkServers(config1));
        assertNotNull(service.getConsumerProperties(config1));
        assertNotNull(service.getProducerProperties(config1));
        assertEquals("localhost:9092",
                service.getProducerProperties(config1).getProperty("bootstrap.servers"));
        assertNotEquals("RANDOM()", service.getTopicPrefix(config1));

        String config2 = "config2";
        assertEquals("remote:2181", service.getZkServers(config2));
        assertNotNull(service.getConsumerProperties(config2));
        assertNotNull(service.getProducerProperties(config2));
        assertEquals("foo", service.getTopicPrefix(config2));

    }
}

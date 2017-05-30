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
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.runtime.test.runner.LocalDeploy;

import java.util.Map;

/**
 * @since 9.2
 */
@LocalDeploy("org.nuxeo.ecm.mqueues.kafka.tests.contrib:test-kafka-config-contrib.xml")
public class TestAutomationKafka extends TestAutomation {

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Override
    public void addExtraParams(Map<String, Object> params) {
        super.addExtraParams(params);
        params.put("kafkaConfig", "default");
    }
}

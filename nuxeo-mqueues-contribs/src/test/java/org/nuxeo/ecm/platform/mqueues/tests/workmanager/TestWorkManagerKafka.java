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
package org.nuxeo.ecm.platform.mqueues.tests.workmanager;

import org.junit.Ignore;
import org.nuxeo.runtime.test.runner.LocalDeploy;

/**
 * @since 9.2
 */
@LocalDeploy({"org.nuxeo.ecm.platform.mqueues.test:test-kafka-config-contrib.xml",
        "org.nuxeo.ecm.platform.mqueues.test:test-workmanager-service.xml",
        "org.nuxeo.ecm.platform.mqueues.test:test-mq-kafka-contrib.xml"})
@Ignore("because we can not use assumption to check if kafka is up")
public class TestWorkManagerKafka extends TestWorkManager {

    @Override
    public String getMQConfig() {
        return "kafka";
    }
}

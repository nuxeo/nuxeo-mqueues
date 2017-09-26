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
package org.nuxeo.ecm.platform.mqueues.tests;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.mqueues.MQService;
import org.nuxeo.lib.core.mqueues.computation.Record;
import org.nuxeo.lib.core.mqueues.mqueues.MQAppender;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.MQRecord;
import org.nuxeo.lib.core.mqueues.mqueues.MQTailer;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @since 9.3
 */
@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy({"org.nuxeo.lib.core.mqueues", "org.nuxeo.ecm.platform.mqueues", "org.nuxeo.ecm.automation.core", "org.nuxeo.ecm.core.io"})
@LocalDeploy("org.nuxeo.ecm.platform.mqueues.test:test-mq-contrib.xml")
public class TestMQService {

    @Test
    public void testMQManagerAccess() {
        MQService service = Framework.getService(MQService.class);
        assertNotNull(service);

        MQManager manager = service.getManager("default");
        assertNotNull(manager);

        manager = service.getManager("import");
        assertNotNull(manager);

        try {
            manager = service.getManager("unknown");
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        manager = service.getManager("default");
        assertNotNull(manager);
        // this config as a mqueue entry that is created if not exists
        manager.exists("input");
        assertEquals(1, manager.getAppender("input").size());
    }


    @Test
    public void testBasicUsage() throws Exception {
        MQService service = Framework.getService(MQService.class);
        MQManager manager = service.getManager("default");
        String mqName = "aqueue";
        String key = "a key";
        String value = "a value";

        try (MQAppender<Record> appender = manager.getAppender(mqName)) {
            appender.append(key, Record.of(key, value.getBytes()));
        }
        try (MQTailer<Record> tailer = manager.createTailer("myGroup", mqName)) {
            MQRecord<Record> mqRecord = tailer.read(Duration.ofSeconds(1));
            assertNotNull(mqRecord);
            assertEquals(key, mqRecord.message().key);
            assertEquals(value, new String(mqRecord.message().data, "UTF-8"));
        }
        // never close the manager this is done by the service
    }

    @Test
    public void testComputationTopology() throws Exception {
        MQService service = Framework.getService(MQService.class);
        MQManager manager = service.getManager("default");
        MQAppender<Record> appender = manager.getAppender("input");
        MQTailer<Record> tailer = manager.createTailer("counter", "output");

        // add an input message
        String key = "a key";
        String value = "a value";
        appender.append(key, Record.of(key, value.getBytes()));

        // the computation should forward this message to the output
        MQRecord<Record> mqRecord = tailer.read(Duration.ofSeconds(1));
        assertNotNull("Record not found in output stream", mqRecord);
        assertEquals(key, mqRecord.message().key);
        assertEquals(value, new String(mqRecord.message().data, "UTF-8"));
    }
}

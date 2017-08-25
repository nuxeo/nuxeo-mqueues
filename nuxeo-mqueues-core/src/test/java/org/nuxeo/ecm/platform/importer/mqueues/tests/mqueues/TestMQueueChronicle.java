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
package org.nuxeo.ecm.platform.importer.mqueues.tests.mqueues;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle.ChronicleMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.keyValueMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * @since 9.2
 */
public class TestMQueueChronicle extends TestMQueue {
    protected Path basePath;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void resetBasePath() throws IOException {
        basePath = null;
    }

    @Override
    public MQManager<keyValueMessage> createManager() throws Exception {
        if (basePath == null) {
            basePath = folder.newFolder().toPath();
        }
        return new ChronicleMQManager<>(basePath, "3s");
    }

    @Test
    public void deleteInvalidPath() throws Exception {
        final int NB_QUEUES = 5;

        ChronicleMQManager<keyValueMessage> manager = (ChronicleMQManager<keyValueMessage>) createManager();
        assertTrue(manager.createIfNotExists("foo", NB_QUEUES));
        String basePath = manager.getBasePath();
        assertTrue(manager.delete("foo"));

        // recreate
        assertTrue(manager.createIfNotExists("foo", NB_QUEUES));
        // add a file in the basePath
        File aFile = new File(basePath, "foo/foo.txt");
        aFile.createNewFile();
        try {
            manager.delete("foo");
            fail("Can not delete a mqueue with external data");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testFileRetention() throws Exception {

        keyValueMessage msg1 = keyValueMessage.of("id1");
        keyValueMessage msg2 = keyValueMessage.of("id2");
        keyValueMessage msg3 = keyValueMessage.of("id3");
        keyValueMessage msg4 = keyValueMessage.of("id4");

        ChronicleMQManager<keyValueMessage> manager = (ChronicleMQManager<keyValueMessage>) createManager();
        manager.createIfNotExists("foo", 1);
        MQAppender<keyValueMessage> appender = manager.getAppender("foo");

        File queueFile = new File(manager.getBasePath(), "foo/Q-00");
        assertEquals(0, queueFile.list().length);

        appender.append(0, msg1);
        assertEquals(1, queueFile.list().length);
        Thread.sleep(1001);
        appender.append(0, msg2);
        assertEquals(2, queueFile.list().length);

        Thread.sleep(4001);

        appender.append(0, msg3);
        assertEquals(3, queueFile.list().length);

        // From now, there should be at least 3 retained files in the queue
        Thread.sleep(1001);
        appender.append(0, msg4);
        assertEquals(3, queueFile.list().length);

    }

}

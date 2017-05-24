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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.ChronicleMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.ChronicleMQueue;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @since 9.2
 */
public class TestMQueueChronicle extends TestMQueue {
    private final static Duration MIN_DURATION = Duration.ofMillis(1);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Override
    public Duration getMinDuration() {
        return MIN_DURATION;
    }

    @Override
    public MQManager<IdMessage> createManager() throws Exception {
        return new ChronicleMQManager<>(folder.newFolder().toPath());
    }

    @Test
    public void openInvalidCQMQueue() throws Exception {
        final int NB_QUEUES = 5;
        String basePath;
        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUES)) {
            assertEquals(NB_QUEUES, mQueue.size());
            basePath = ((ChronicleMQueue<IdMessage>) mQueue).getBasePath();
        }
        // add a file in the basePath
        File aFile = new File(basePath, "foo.txt");
        aFile.createNewFile();

        // create a new mqueues but fails because it does not looks like a mqueues
        try (MQueue<IdMessage> mQueue = createMQ(1)) {
            fail("Create a mqueue on an existing folder with extra data is not allowed");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

}

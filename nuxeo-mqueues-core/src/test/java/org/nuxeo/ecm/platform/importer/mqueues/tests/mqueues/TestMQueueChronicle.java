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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle.ChronicleMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * @since 9.2
 */
public class TestMQueueChronicle extends TestMQueue {
    private final static Duration MIN_DURATION = Duration.ofMillis(1);
    private Path basePath;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Override
    public Duration getMinDuration() {
        return MIN_DURATION;
    }

    @After
    public void resetBasePath() throws IOException {
        basePath = null;
    }

    @Override
    public MQManager<IdMessage> createManager() throws Exception {
        if (basePath == null) {
            basePath = folder.newFolder().toPath();
        }
        return new ChronicleMQManager<>(basePath);
    }

    @Test
    public void deleteInvalidPath() throws Exception {
        final int NB_QUEUES = 5;

        ChronicleMQManager<IdMessage> manager = (ChronicleMQManager<IdMessage>) createManager();
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

}

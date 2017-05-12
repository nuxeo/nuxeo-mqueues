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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @since 9.2
 */
public class TestMQueuesChronicle extends TestMQueues {
    private File basePath;
    private final static Duration MIN_DURATION = Duration.ofMillis(1);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public
    TestName name = new TestName();

    @Override
    public MQueues<IdMessage> createMQ(int partitions) throws Exception {
        basePath = folder.newFolder(name.getMethodName());
        if (CQMQueues.exists(basePath)) {
            CQMQueues.delete(basePath);
        }
        return CQMQueues.create(basePath, partitions);
    }

    @Override
    public MQueues<IdMessage> reopenMQ() {
        return CQMQueues.open(basePath);
    }

    @Override
    public MQueues<IdMessage> createSameMQ(int partitions) throws Exception {
        return CQMQueues.create(basePath, partitions);
    }

    @Override
    public Duration getMinDuration() {
        return MIN_DURATION;
    }

    @Test
    public void openInvalidCQMQueues() throws Exception {
        final int NB_QUEUES = 5;

        try (MQueues<IdMessage> mQueues = createMQ(NB_QUEUES)) {
            assertEquals(NB_QUEUES, mQueues.size());
        }
        // add a file in the basePath
        File aFile = new File(basePath, "foo.txt");
        aFile.createNewFile();

        // create a new mqueues but fails because it does not looks like a mqueues
        try (MQueues<IdMessage> mQueues = createSameMQ(1)) {
            fail("Create a mqueue on an existing folder with extra data is not allowed");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

}

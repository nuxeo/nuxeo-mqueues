/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 */
package org.nuxeo.ecm.platform.importer.mqueues.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.CQMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestCQMQueues {

    protected static final Log log = LogFactory.getLog(TestCQMQueues.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void basic() throws Exception {
        final int NB_QUEUE = 10;
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(folder.newFolder("cq"), NB_QUEUE)) {
            assertEquals(NB_QUEUE, mQueues.size());
            IdMessage msg1 = new IdMessage("id1");
            IdMessage msg2 = new IdMessage("id2");

            mQueues.put(1, msg1);
            MQueues.Tailer<IdMessage> tailer1 = mQueues.getTailer(1);
            assertEquals(msg1, tailer1.get(1, TimeUnit.MILLISECONDS));
            assertEquals(null, tailer1.get(1, TimeUnit.MILLISECONDS));

            mQueues.put(2, msg2);
            assertEquals(null, tailer1.get(1, TimeUnit.MILLISECONDS));

            mQueues.put(1, msg2);
            assertEquals(msg2, tailer1.get(1, TimeUnit.SECONDS));

            assertEquals(msg2, mQueues.getTailer(2).get(1, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void commitOffset() throws Exception {
        final int NB_QUEUE = 10;
        final File basePath = folder.newFolder("cq");
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath, NB_QUEUE)) {
            mQueues.put(1, new IdMessage("id1"));
            mQueues.put(1, new IdMessage("id2"));
            mQueues.put(1, new IdMessage("id3"));

            mQueues.put(2, new IdMessage("id4"));
            mQueues.put(2, new IdMessage("id5"));

            // process 2 messages and commit on tailer1
            MQueues.Tailer<IdMessage> tailer = mQueues.getTailer(1);
            assertEquals("id1", tailer.get(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();
            assertEquals("id2", tailer.get(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();

            MQueues.Tailer<IdMessage> tailer2 = mQueues.getTailer(2);
            assertEquals("id4", tailer2.get(2, TimeUnit.MILLISECONDS).getId());
            tailer2.commit();
            tailer2.commit();
        }

        // reopen the same queues in append mode
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath)) {
            MQueues.Tailer<IdMessage> tailer = mQueues.getTailer(1);
            tailer.toStart();
            assertEquals("id1", tailer.get(1, TimeUnit.MILLISECONDS).getId());

            tailer.toEnd();
            assertEquals(null, tailer.get(1, TimeUnit.MILLISECONDS));

            tailer.toLastCommitted();
            assertEquals("id3", tailer.get(1, TimeUnit.MILLISECONDS).getId());

            // by default to lastCommit
            MQueues.Tailer<IdMessage> tailer2 = mQueues.getTailer(2);
            assertEquals("id5", tailer2.get(1, TimeUnit.MILLISECONDS).getId());

            tailer2.toStart();
            assertEquals("id4", tailer2.get(1, TimeUnit.MILLISECONDS).getId());
        }

    }


    @Test
    public void commitOffset2() throws Exception {
        final int NB_QUEUE = 10;
        final File basePath = folder.newFolder("cq");
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath, NB_QUEUE)) {
            mQueues.put(1, new IdMessage("id1"));
            mQueues.put(1, new IdMessage("id2"));
            mQueues.put(1, new IdMessage("id3"));
            mQueues.put(1, new IdMessage("id4"));

            MQueues.Tailer<IdMessage> tailer = mQueues.getTailer(1);
            assertEquals("id1", tailer.get(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();
            assertEquals("id2", tailer.get(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();

            // restart from the beginning and commit after the first message
            tailer.toStart();
            assertEquals("id1", tailer.get(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();
        }

        // reopen the same queues in append mode
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath)) {
            MQueues.Tailer<IdMessage> tailer = mQueues.getTailer(1);
            tailer.toLastCommitted();
            // the last committed message was id1
            assertEquals("id2", tailer.get(1, TimeUnit.MILLISECONDS).getId());
        }

    }

}

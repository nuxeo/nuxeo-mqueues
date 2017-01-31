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
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCQMQueues {

    protected static final Log log = LogFactory.getLog(TestCQMQueues.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public CQMQueues<IdMessage> createMQ(File basePath, int NB_QUEUES) {
        return new CQMQueues<>(basePath, NB_QUEUES);
    }

    public CQMQueues<IdMessage> openMQ(File basePath) {
        return new CQMQueues<>(basePath);
    }


    @Test
    public void open() throws Exception {
        final File basePath = folder.newFolder("cq");
        final int NB_QUEUES = 5;

        // check that the number of queues is persisted even if we don't write anything
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUES)) {
            assertEquals(NB_QUEUES, mQueues.size());
        }
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            assertEquals(NB_QUEUES, mQueues.size());
        }

        // same with another size
        try (MQueues<IdMessage> mQueues = createMQ(basePath, 1)) {
            assertEquals(1, mQueues.size());
        }
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            assertEquals(1, mQueues.size());
        }

        // add a file in the basePath
        File aFile = new File(basePath, "foo.txt");
        aFile.createNewFile();

        // create a new mqueues but fails because it does not looks like a mqueues
        try (MQueues<IdMessage> mQueues = createMQ(basePath, 1)) {
            fail("Create a mqueue on an existing folder with extra data is not allowed");
        } catch (IllegalArgumentException e) {
            // expected
        }


    }

    @Test
    public void basicAppendAndTail() throws Exception {
        final int NB_QUEUE = 10;
        IdMessage msg1 = new IdMessage("id1");
        IdMessage msg2 = new IdMessage("id2");
        File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mq = createMQ(basePath, NB_QUEUE)) {
            mq.append(1, msg1);

            MQueues.Tailer<IdMessage> tailer1 = mq.createTailer(1);
            assertEquals(msg1, tailer1.read(1, TimeUnit.MILLISECONDS));
            assertEquals(null, tailer1.read(1, TimeUnit.MILLISECONDS));

            mq.append(2, msg2);
            assertEquals(null, tailer1.read(1, TimeUnit.MILLISECONDS));

            mq.append(1, msg2);
            assertEquals(msg2, tailer1.read(1, TimeUnit.SECONDS));

            assertEquals(msg2, mq.createTailer(2).read(1, TimeUnit.MILLISECONDS));
        }

        // open the mqueue offset consumer starts at the beginning because tailer have not committed.
        try (MQueues<IdMessage> mq = openMQ(basePath)) {
            MQueues.Tailer<IdMessage> tailer1 = mq.createTailer(1);
            MQueues.Tailer<IdMessage> tailer2 = mq.createTailer(2);

            assertEquals(msg1, tailer1.read(1, TimeUnit.MILLISECONDS));
            assertEquals(msg2, tailer1.read(1, TimeUnit.MILLISECONDS));
            assertEquals(null, tailer1.read(1, TimeUnit.MILLISECONDS));

            assertEquals(msg2, tailer2.read(1, TimeUnit.MILLISECONDS));
            assertEquals(null, tailer2.read(1, TimeUnit.MILLISECONDS));
        }


    }

    @Test
    public void commitOffset() throws Exception {
        final int NB_QUEUE = 10;
        final File basePath = folder.newFolder("cq");
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.append(1, new IdMessage("id1"));
            mQueues.append(1, new IdMessage("id2"));
            mQueues.append(1, new IdMessage("id3"));

            mQueues.append(2, new IdMessage("id4"));
            mQueues.append(2, new IdMessage("id5"));

            // process 2 messages and commit on tailer1
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1);
            assertEquals("id1", tailer.read(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();
            assertEquals("id2", tailer.read(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();

            MQueues.Tailer<IdMessage> tailer2 = mQueues.createTailer(2);
            assertEquals("id4", tailer2.read(2, TimeUnit.MILLISECONDS).getId());
            tailer2.commit();
            tailer2.commit();
        }

        // reopen the same queues in append mode
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1);
            tailer.toStart();
            assertEquals("id1", tailer.read(1, TimeUnit.MILLISECONDS).getId());

            tailer.toEnd();
            assertEquals(null, tailer.read(1, TimeUnit.MILLISECONDS));

            tailer.toLastCommitted();
            assertEquals("id3", tailer.read(1, TimeUnit.MILLISECONDS).getId());

            // by default to lastCommit
            MQueues.Tailer<IdMessage> tailer2 = mQueues.createTailer(2);
            assertEquals("id5", tailer2.read(1, TimeUnit.MILLISECONDS).getId());

            tailer2.toStart();
            assertEquals("id4", tailer2.read(1, TimeUnit.MILLISECONDS).getId());
        }

    }

    @Test
    public void commitOffset2() throws Exception {
        final int NB_QUEUE = 10;
        final File basePath = folder.newFolder("cq");
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.append(1, new IdMessage("id1"));
            mQueues.append(1, new IdMessage("id2"));
            mQueues.append(1, new IdMessage("id3"));
            mQueues.append(1, new IdMessage("id4"));

            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1);
            assertEquals("id1", tailer.read(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();
            assertEquals("id2", tailer.read(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();

            // restart from the beginning and commit after the first message
            tailer.toStart();
            assertEquals("id1", tailer.read(1, TimeUnit.MILLISECONDS).getId());
            tailer.commit();
        }

        // reopen
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1);
            tailer.toLastCommitted();
            // the last committed message was id1
            assertEquals("id2", tailer.read(1, TimeUnit.MILLISECONDS).getId());
        }

    }

    @Test
    public void commitOffsetNameSpace() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");
        final IdMessage msg1 = new IdMessage("id1");
        final IdMessage msg2 = new IdMessage("id2");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            for (int i = 0; i < 10; i++) {
                mQueues.append(0, new IdMessage("id" + i));
            }
            // each tailers have distincts commit offsets
            MQueues.Tailer<IdMessage> tailer0a = mQueues.createTailer(0, "a");
            MQueues.Tailer<IdMessage> tailer0b = mQueues.createTailer(0, "b");

            assertEquals("id0", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id1", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            tailer0a.commit();
            assertEquals("id2", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id3", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            tailer0a.toLastCommitted();
            assertEquals("id2", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id3", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());


            assertEquals("id0", tailer0b.read(0, TimeUnit.MILLISECONDS).getId());
            tailer0b.commit();
            assertEquals("id1", tailer0b.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id2", tailer0b.read(0, TimeUnit.MILLISECONDS).getId());

            tailer0b.toLastCommitted();
            assertEquals("id1", tailer0b.read(0, TimeUnit.MILLISECONDS).getId());

            tailer0a.toLastCommitted();
            assertEquals("id2", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
        }

        // reopen
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
            assertEquals("id0", tailer.read(0, TimeUnit.MILLISECONDS).getId());

            tailer = mQueues.createTailer(0, "a");
            assertEquals("id2", tailer.read(0, TimeUnit.MILLISECONDS).getId());

            tailer = mQueues.createTailer(0, "b");
            assertEquals("id1", tailer.read(0, TimeUnit.MILLISECONDS).getId());
        }


    }

    @Test
    public void commitConccurentTailer() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            for (int i = 0; i < 10; i++) {
                mQueues.append(0, new IdMessage("id" + i));
            }

            // both tailer share the same commit offset
            MQueues.Tailer<IdMessage> tailer0a = mQueues.createTailer(0);

            assertEquals("id0", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id1", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            tailer0a.commit();
            assertEquals("id2", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id3", tailer0a.read(0, TimeUnit.MILLISECONDS).getId());

            MQueues.Tailer<IdMessage> tailer0b = mQueues.createTailer(0);
            assertEquals("id2", tailer0b.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id3", tailer0b.read(0, TimeUnit.MILLISECONDS).getId());
            assertEquals("id4", tailer0b.read(0, TimeUnit.MILLISECONDS).getId());
            tailer0b.commit();
        }

        // reopen the last commit win
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
            assertEquals("id5", tailer.read(0, TimeUnit.MILLISECONDS).getId());
        }
    }


    @Test
    public void waitForConsumer() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        Offset offset = null;
        Offset offset0 = null;
        Offset offset5 = null;
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            // appends some msg and keep some offsets
            for (int i = 0; i < 10; i++) {
                offset = mQueues.append(0, new IdMessage("id" + i));
                if (i == 0) {
                    offset0 = offset;
                } else if (i == 5) {
                    offset5 = offset;
                }
            }
            // nothing committed
            assertFalse(mQueues.waitFor(offset, 0, TimeUnit.MILLISECONDS));
            assertFalse(mQueues.waitFor(offset0, 0, TimeUnit.MILLISECONDS));
            assertFalse(mQueues.waitFor(offset5, 0, TimeUnit.MILLISECONDS));

            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
            tailer.read(0, TimeUnit.MILLISECONDS);
            tailer.commit();

            // msg 0 is processed and committed
            assertTrue(mQueues.waitFor(offset0, 0, TimeUnit.MILLISECONDS));
            // msg 5 and last is processed and committed
            assertFalse(mQueues.waitFor(offset5, 0, TimeUnit.MILLISECONDS));
            assertFalse(mQueues.waitFor(offset, 0, TimeUnit.MILLISECONDS));

            // drain
            while(tailer.read(0, TimeUnit.MILLISECONDS) != null);

            // message is processed but not yet committed
            assertFalse(mQueues.waitFor(offset, 0, TimeUnit.MILLISECONDS));
            tailer.commit();

            // message is processed and committed
            assertTrue(mQueues.waitFor(offset0, 0, TimeUnit.MILLISECONDS));
            assertTrue(mQueues.waitFor(offset5, 0, TimeUnit.MILLISECONDS));
            assertTrue(mQueues.waitFor(offset, 0, TimeUnit.MILLISECONDS));

        }
    }
}

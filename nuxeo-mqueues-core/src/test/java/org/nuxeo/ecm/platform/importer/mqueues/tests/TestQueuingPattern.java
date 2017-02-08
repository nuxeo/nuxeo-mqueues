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

import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.CQMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;
import org.nuxeo.ecm.platform.importer.mqueues.tests.consumer.IdMessageFactory;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestQueuingPattern {

    protected static final Log log = LogFactory.getLog(TestQueuingPattern.class);
    private static final RetryPolicy NO_RETRY = new RetryPolicy().withMaxRetries(0);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void endWithPoisonPill() throws Exception {
        final int NB_QUEUE = 2;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mq = new CQMQueues<>(basePath, NB_QUEUE)) {
            // run the consumers pool
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                    new IdMessageFactory(IdMessageFactory.ConsumerType.NOOP),
                    new ConsumerPolicy.Builder().continueOnFailure(true).waitMessageForEver().build());

            CompletableFuture<List<ConsumerStatus>> consumersFuture = consumers.start();

            // submit messages
            Offset offset1 = mq.append(0, new IdMessage("id1"));
            // may be processed but not commited because batch is not full
            assertFalse(mq.waitFor(offset1, Duration.ofMillis(0)));
            // send a force batch
            mq.append(0, new IdMessage("batch now", false, true));
            assertTrue(mq.waitFor(offset1, Duration.ofSeconds(1)));

            // terminate consumers
            mq.append(0, new IdMessage("poison pill", true, false));
            mq.append(1, new IdMessage("poison pill", true, false));

            List<ConsumerStatus> ret = consumersFuture.get();
            assertEquals(NB_QUEUE, ret.stream().count());
            assertEquals(4, ret.stream().mapToLong(r -> r.committed).sum());
        }
    }

    @Test
    public void killConsumers() throws Exception {
        final int NB_QUEUE = 2;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mq = new CQMQueues<>(basePath, NB_QUEUE)) {
            // run the consumers pool
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                    new IdMessageFactory(IdMessageFactory.ConsumerType.NOOP),
                    new ConsumerPolicy.Builder().continueOnFailure(true).waitMessageForEver().build());
            CompletableFuture<List<ConsumerStatus>> future = consumers.start();

            // submit messages
            Offset offset1 = mq.append(0, new IdMessage("id1"));
            // may be processed but not commited because batch is not full
            assertFalse(mq.waitFor(offset1, Duration.ofMillis(0)));
            // send a force batch
            mq.append(0, new IdMessage("batch now", false, true));
            assertTrue(mq.waitFor(offset1, Duration.ofSeconds(1)));

            // send 2 more messages
            mq.append(0, new IdMessage("foo"));
            mq.append(0, new IdMessage("foo"));

            // terminate consumers by shuting down the thread pool
            consumers.shutdownNow();

            List<ConsumerStatus> ret = future.get();
            assertEquals(NB_QUEUE, ret.stream().count());
            assertEquals(2, ret.stream().filter(s -> s.fail).count());
        }

        try (MQueues<IdMessage> mq = new CQMQueues<>(basePath)) {
            // run the consumers pool again
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                    new IdMessageFactory(IdMessageFactory.ConsumerType.NOOP),
                    new ConsumerPolicy.Builder().continueOnFailure(true).waitMessageForEver().build());
            CompletableFuture<List<ConsumerStatus>> future = consumers.start();
            // terminate the consumers with pills
            mq.append(0, new IdMessage("poison pills", true, true));
            mq.append(1, new IdMessage("poison pills", true, true));

            List<ConsumerStatus> ret = future.get();
            // 2 messages from the previous run + 2 poison pills
            assertEquals(4, ret.stream().mapToLong(r -> r.committed).sum());
        }


    }
}
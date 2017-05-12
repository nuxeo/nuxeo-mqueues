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
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;
import org.nuxeo.ecm.platform.importer.mqueues.tests.consumer.IdMessageFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public abstract class TestPatternQueuing {
    protected static final Log log = LogFactory.getLog(TestPatternQueuing.class);

    public abstract MQueues<IdMessage> createMQ(int partitions) throws Exception;

    public abstract MQueues<IdMessage> reopenMQ();

    @Test
    public void endWithPoisonPill() throws Exception {
        final int NB_QUEUE = 2;

        try (MQueues<IdMessage> mq = createMQ(NB_QUEUE);
             ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                     IdMessageFactory.NOOP,
                     ConsumerPolicy.UNBOUNDED)) {
            // run the consumers pool
            CompletableFuture<List<ConsumerStatus>> consumersFuture = consumers.start();

            // submit messages
            Offset offset1 = mq.append(0, IdMessage.of("id1"));
            // may be processed but not committed because batch is not full
            assertFalse(mq.waitFor(offset1, Duration.ofMillis(0)));
            // send a force batch
            mq.append(0, IdMessage.ofForceBatch("batch now"));
            assertTrue(mq.waitFor(offset1, Duration.ofSeconds(10)));

            // terminate consumers
            mq.append(0, IdMessage.POISON_PILL);
            mq.append(1, IdMessage.POISON_PILL);

            List<ConsumerStatus> ret = consumersFuture.get();
            assertEquals(NB_QUEUE, (long) ret.size());
            assertEquals(2, ret.stream().mapToLong(r -> r.committed).sum());
        }
    }

    @Test
    public void endWithPoisonPillCommitTheBatch() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueues<IdMessage> mq = createMQ(NB_QUEUE);
             ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                     IdMessageFactory.NOOP,
                     ConsumerPolicy.UNBOUNDED)) {
            // run the consumers pool
            CompletableFuture<List<ConsumerStatus>> consumersFuture = consumers.start();

            // submit messages
            mq.append(0, IdMessage.of("id1"));
            // terminate consumers
            mq.append(0, IdMessage.POISON_PILL);
            mq.append(0, IdMessage.of("no consumer to read this one"));
            List<ConsumerStatus> ret = consumersFuture.get();
            assertEquals(NB_QUEUE, (long) ret.size());
            assertEquals(1, ret.stream().mapToLong(r -> r.batchCommit).sum());
            assertEquals(1, ret.stream().mapToLong(r -> r.committed).sum());
        }
    }

    @Test
    public void killConsumers() throws Exception {
        final int NB_QUEUE = 2;

        try (MQueues<IdMessage> mq = createMQ(NB_QUEUE);
             ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                     IdMessageFactory.NOOP,
                     ConsumerPolicy.UNBOUNDED)) {
            // run the consumers pool
            CompletableFuture<List<ConsumerStatus>> future = consumers.start();

            // submit messages
            Offset offset1 = mq.append(0, IdMessage.of("id1"));
            // may be processed but not committed because batch is not full
            assertFalse(mq.waitFor(offset1, Duration.ofMillis(0)));
            // send a force batch
            mq.append(0, IdMessage.ofForceBatch("batch now"));
            assertTrue(mq.waitFor(offset1, Duration.ofSeconds(10)));

            // send 2 more messages
            mq.append(0, IdMessage.of("foo"));
            mq.append(0, IdMessage.of("foo"));

            // terminate consumers abruptly without committing the last message
            consumers.close();

            List<ConsumerStatus> ret = future.get();
            assertEquals(NB_QUEUE, (long) ret.size());
            assertEquals(2, ret.stream().filter(s -> s.fail).count());
        }

        try (MQueues<IdMessage> mq = reopenMQ();
             ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                     IdMessageFactory.NOOP,
                     ConsumerPolicy.UNBOUNDED)) {
            // run the consumers pool again
            CompletableFuture<List<ConsumerStatus>> future = consumers.start();
            // terminate the consumers with pills
            mq.append(0, IdMessage.POISON_PILL);
            mq.append(1, IdMessage.POISON_PILL);

            List<ConsumerStatus> ret = future.get();
            // 2 messages from the previous run, poison pill are not counted
            assertEquals(2, ret.stream().mapToLong(r -> r.committed).sum());
        }

    }

    @Test
    public void killMQueues() throws Exception {
        final int NB_QUEUE = 2;

        try (MQueues<IdMessage> mq = createMQ(NB_QUEUE)) {
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                    IdMessageFactory.NOOP,
                    ConsumerPolicy.UNBOUNDED);
            // run the consumers pool
            CompletableFuture<List<ConsumerStatus>> future = consumers.start();

            // submit messages
            Offset offset1 = mq.append(0, IdMessage.of("id1"));
            // may be processed but not committed because batch is not full
            assertFalse(mq.waitFor(offset1, Duration.ofMillis(0)));
            // send a force batch
            mq.append(0, IdMessage.ofForceBatch("batch now"));
            assertTrue(mq.waitFor(offset1, Duration.ofSeconds(10)));

            // send 2 more messages
            mq.append(0, IdMessage.of("foo"));
            mq.append(0, IdMessage.of("foo"));
            // close the mq
            mq.close();

            // open a new mq
            try (MQueues<IdMessage> mqBis = reopenMQ()) {
                mqBis.append(0, IdMessage.ofForceBatch("force batch"));
            }
            // the consumers should be in error because their tailer are associated to a closed mqueues
            List<ConsumerStatus> ret = future.get();
            // 2 failures
            assertEquals(2, ret.stream().filter(r -> r.fail).count());
        }

        // restart the mq and consumers
        try (MQueues<IdMessage> mq = reopenMQ()) {
            // run the consumers pool again
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mq,
                    IdMessageFactory.NOOP,
                    ConsumerPolicy.builder().continueOnFailure(true).waitMessageForEver().build());
            CompletableFuture<List<ConsumerStatus>> future = consumers.start();
            // terminate the consumers with pills
            mq.append(0, IdMessage.POISON_PILL);
            mq.append(1, IdMessage.POISON_PILL);

            List<ConsumerStatus> ret = future.get();
            // 3 messages from the previous run + 2 poison pills
            assertEquals(3, ret.stream().mapToLong(r -> r.committed).sum());
        }

    }
}
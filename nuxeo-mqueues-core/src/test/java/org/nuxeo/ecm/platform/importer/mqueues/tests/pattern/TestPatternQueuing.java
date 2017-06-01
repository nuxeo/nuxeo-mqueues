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
package org.nuxeo.ecm.platform.importer.mqueues.tests.pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.tests.pattern.consumer.IdMessageFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public abstract class TestPatternQueuing {
    protected static final Log log = LogFactory.getLog(TestPatternQueuing.class);
    protected String mqName = "mqName";

    @Rule
    public
    TestName name = new TestName();

    private MQManager<IdMessage> manager;

    public abstract MQManager<IdMessage> createManager() throws Exception;


    @Before
    public void initManager() throws Exception {
        mqName = name.getMethodName();
        if (manager == null) {
            manager = createManager();
        }
    }

    @After
    public void closeManager() throws Exception {
        if (manager != null) {
            manager.close();
        }
        manager = null;
    }

    public void resetManager() throws Exception {
        closeManager();
        initManager();
    }

    @Test
    public void endWithPoisonPill() throws Exception {
        final int NB_QUEUE = 2;

        manager.createIfNotExists(mqName, NB_QUEUE);

        ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mqName, manager,
                IdMessageFactory.NOOP,
                ConsumerPolicy.UNBOUNDED);
        // run the consumers pool
        CompletableFuture<List<ConsumerStatus>> consumersFuture = consumers.start();

        MQAppender<IdMessage> appender = manager.getAppender(mqName);
        // submit messages
        MQOffset offset1 = appender.append(0, IdMessage.of("id1"));
        // may be processed but not committed because batch is not full
        assertFalse(appender.waitFor(offset1, consumers.getConsumerGroupName(), Duration.ofMillis(0)));
        // send a force batch
        appender.append(0, IdMessage.ofForceBatch("batch now"));
        assertTrue(appender.waitFor(offset1, consumers.getConsumerGroupName(), Duration.ofSeconds(10)));

        // terminate consumers
        appender.append(0, IdMessage.POISON_PILL);
        appender.append(1, IdMessage.POISON_PILL);

        List<ConsumerStatus> ret = consumersFuture.get();
        assertEquals(NB_QUEUE, (long) ret.size());
        assertEquals(2, ret.stream().mapToLong(r -> r.committed).sum());
    }

    @Test
    public void endWithPoisonPillCommitTheBatch() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mqName, manager,
                IdMessageFactory.NOOP,
                ConsumerPolicy.UNBOUNDED);
        // run the consumers pool
        CompletableFuture<List<ConsumerStatus>> consumersFuture = consumers.start();

        // submit messages
        MQAppender<IdMessage> appender = manager.getAppender(mqName);
        appender.append(0, IdMessage.of("id1"));
        // terminate consumers
        appender.append(0, IdMessage.POISON_PILL);
        appender.append(0, IdMessage.of("no consumer to read this one"));
        List<ConsumerStatus> ret = consumersFuture.get();
        assertEquals(NB_QUEUE, (long) ret.size());
        assertEquals(1, ret.stream().mapToLong(r -> r.batchCommit).sum());
        assertEquals(1, ret.stream().mapToLong(r -> r.committed).sum());
    }

    @Test
    public void killConsumers() throws Exception {
        final int NB_QUEUE = 2;
        manager.createIfNotExists(mqName, NB_QUEUE);
        ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mqName, manager,
                IdMessageFactory.NOOP,
                ConsumerPolicy.UNBOUNDED);
        // run the consumers pool
        CompletableFuture<List<ConsumerStatus>> future = consumers.start();

        // submit messages
        MQAppender<IdMessage> appender = manager.getAppender(mqName);
        MQOffset offset1 = appender.append(0, IdMessage.of("id1"));

        // may be processed but not committed because batch is not full
        assertFalse(appender.waitFor(offset1, consumers.getConsumerGroupName(), Duration.ofMillis(0)));
        // send a force batch
        appender.append(0, IdMessage.ofForceBatch("batch now"));
        assertTrue(appender.waitFor(offset1, consumers.getConsumerGroupName(), Duration.ofSeconds(10)));

        // send 2 more messages
        appender.append(0, IdMessage.of("foo"));
        appender.append(0, IdMessage.of("foo"));

        // terminate consumers abruptly without committing the last message
        consumers.close();

        List<ConsumerStatus> ret = future.get();
        assertEquals(NB_QUEUE, (long) ret.size());
        assertEquals(2, ret.stream().filter(s -> s.fail).count());

        // reset manager
        resetManager();

        consumers = new ConsumerPool<>(mqName, manager, IdMessageFactory.NOOP,
                ConsumerPolicy.UNBOUNDED);
        // run the consumers pool again
        future = consumers.start();
        appender = manager.getAppender(mqName);
        // terminate the consumers with pills
        appender.append(0, IdMessage.POISON_PILL);
        appender.append(1, IdMessage.POISON_PILL);

        ret = future.get();
        // 2 messages from the previous run, poison pill are not counted
        assertEquals(2, ret.stream().mapToLong(r -> r.committed).sum());
    }

    @Test
    public void killMQueue() throws Exception {
        final int NB_QUEUE = 2;
        manager.createIfNotExists(mqName, NB_QUEUE);

        ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mqName, manager,
                IdMessageFactory.NOOP,
                ConsumerPolicy.UNBOUNDED);
        // run the consumers pool
        CompletableFuture<List<ConsumerStatus>> future = consumers.start();

        // submit messages
        MQAppender<IdMessage> appender = manager.getAppender(mqName);
        MQOffset offset1 = appender.append(0, IdMessage.of("id1"));
        // may be processed but not committed because batch is not full
        assertFalse(appender.waitFor(offset1, consumers.getConsumerGroupName(), Duration.ofMillis(0)));
        // send a force batch
        appender.append(0, IdMessage.ofForceBatch("batch now"));
        assertTrue(appender.waitFor(offset1, consumers.getConsumerGroupName(), Duration.ofSeconds(10)));

        // send 2 more messages
        appender.append(0, IdMessage.of("foo"));
        appender.append(0, IdMessage.of("foo"));
        // close the mq

        resetManager();

        appender = manager.getAppender(mqName);
        // open a new mq

        appender.append(0, IdMessage.ofForceBatch("force batch"));

        // the consumers should be in error because their tailer are associated to a closed mqueues
        List<ConsumerStatus> ret = future.get();
        // 2 failures
        assertEquals(2, ret.stream().filter(r -> r.fail).count());


        // run the consumers pool again
        consumers = new ConsumerPool<>(mqName, manager,
                IdMessageFactory.NOOP,
                ConsumerPolicy.builder().continueOnFailure(true).waitMessageForEver().build());
        future = consumers.start();
        // terminate the consumers with pills
        appender.append(0, IdMessage.POISON_PILL);
        appender.append(1, IdMessage.POISON_PILL);

        ret = future.get();
        // 3 messages from the previous run + 2 poison pills
        assertEquals(3, ret.stream().mapToLong(r -> r.committed).sum());
    }


}
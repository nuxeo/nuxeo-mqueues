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

import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.BatchPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.tests.pattern.consumer.IdMessageFactory;
import org.nuxeo.ecm.platform.importer.mqueues.tests.pattern.producer.RandomIdMessageProducerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public abstract class TestPatternBoundedQueuing {
    protected static final Log log = LogFactory.getLog(TestPatternBoundedQueuing.class);

    @Rule
    public TestName name = new TestName();

    private MQManager<IdMessage> manager;

    public abstract MQManager<IdMessage> createManager() throws Exception;

    public MQManager<IdMessage> getManager() throws Exception {
        if (manager == null) {
            manager = createManager();
        }
        return manager;
    }

    public MQueue<IdMessage> createMQ(int size) throws Exception {
        return getManager().create(name.getMethodName(), size);
    }

    public MQueue<IdMessage> reopenMQ() throws Exception {
        return getManager().open(name.getMethodName());
    }

    @After
    public void resetManager() throws Exception {
        if (manager != null) {
            manager.close();
        }
        manager = null;
    }


    @Test
    public void producersThenConsumers() throws Exception {
        final int NB_QUEUE = 10;
        final int NB_PRODUCERS = 15;
        final int NB_DOCUMENTS = 1 * 1000;

        // 1. Create a mq and run the producers
        List<ProducerStatus> pret;
        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE);
             ProducerPool<IdMessage> producers = new ProducerPool<>(mQueue,
                     new RandomIdMessageProducerFactory(NB_DOCUMENTS), NB_PRODUCERS)) {
            pret = producers.start().get();
        }
        assertEquals(NB_PRODUCERS, (long) pret.size());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, pret.stream().mapToLong(r -> r.nbProcessed).sum());

        // 2. Use the mq and run the consumers
        List<ConsumerStatus> cret;
        try (MQueue<IdMessage> mQueue = reopenMQ();
             ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mQueue,
                     IdMessageFactory.NOOP, ConsumerPolicy.BOUNDED)) {
            cret = consumers.start().get();
        }
        assertEquals(NB_QUEUE, (long) cret.size());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, cret.stream().mapToLong(r -> r.committed).sum());
    }

    @Test
    public void producersAndConsumersConcurrently() throws Exception {
        final int NB_QUEUE = 10;
        final int NB_PRODUCERS = 15;
        final int NB_DOCUMENTS = 1000;
        List<ProducerStatus> pret;
        List<ConsumerStatus> cret;
        // Create a mq, producer and consumer pool
        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE);
             ProducerPool<IdMessage> producers = new ProducerPool<>(mQueue,
                     new RandomIdMessageProducerFactory(NB_DOCUMENTS), NB_PRODUCERS);
             ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mQueue,
                     IdMessageFactory.NOOP, ConsumerPolicy.BOUNDED)) {
            CompletableFuture<List<ProducerStatus>> pfuture = producers.start();
            CompletableFuture<List<ConsumerStatus>> cfuture = consumers.start();
            // wait for the completion
            cret = cfuture.get();
            pret = pfuture.get();
        }
        assertEquals(NB_PRODUCERS, (long) pret.size());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, pret.stream().mapToLong(r -> r.nbProcessed).sum());

        assertEquals(NB_QUEUE, (long) cret.size());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, cret.stream().mapToLong(r -> r.committed).sum());

    }

    @Test
    public void producerAndBuggyConsumers() throws Exception {
        final int NB_QUEUE = 23;
        // ordered message producer requires nb_producer <= nb consumer
        final int NB_PRODUCERS = NB_QUEUE;
        final int NB_DOCUMENTS = getNbDocumentForBuggyConsumerTest();
        // final int NB_DOCUMENTS = 499999;
        final int BATCH_SIZE = 13;
        List<ProducerStatus> pret;
        List<ConsumerStatus> cret;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE);
             ProducerPool<IdMessage> producers = new ProducerPool<>(mQueue,
                     new RandomIdMessageProducerFactory(NB_DOCUMENTS,
                             RandomIdMessageProducerFactory.ProducerType.ORDERED),
                     NB_PRODUCERS)) {
            pret = producers.start().get();
        }
        assertEquals(NB_PRODUCERS, (long) pret.size());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, pret.stream().mapToLong(r -> r.nbProcessed).sum());

        // 2. Use the mq and run the consumers
        try (MQueue<IdMessage> mQueue = reopenMQ();
             ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mQueue,
                     IdMessageFactory.BUGGY,
                     ConsumerPolicy.builder()
                             .batchPolicy(BatchPolicy.builder().capacity(BATCH_SIZE).build())
                             .retryPolicy(new RetryPolicy().withMaxRetries(1000)).build())) {
            cret = consumers.start().get();
        }
        assertEquals(NB_QUEUE, (long) cret.size());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, cret.stream().mapToLong(r -> r.committed).sum());
        assertTrue(NB_PRODUCERS * NB_DOCUMENTS < cret.stream().mapToLong(r -> r.accepted).sum());
    }

    public int getNbDocumentForBuggyConsumerTest() {
        return 10151;
    }

}

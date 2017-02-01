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
import org.nuxeo.ecm.platform.importer.mqueues.consumer.BatchPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.CQMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.producer.ProducerPool;
import org.nuxeo.ecm.platform.importer.mqueues.producer.ProducerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.tests.consumer.IdMessageFactory;
import org.nuxeo.ecm.platform.importer.mqueues.tests.producer.RandomIdMessageProducerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestQueuingPattern {

    protected static final Log log = LogFactory.getLog(TestQueuingPattern.class);
    private static final RetryPolicy NO_RETRY = new RetryPolicy().withMaxRetries(0);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void producersThenConsumers() throws Exception {
        final int NB_QUEUE = 10;
        final int NB_PRODUCERS = 15;
        final int NB_DOCUMENTS = 1 * 1000;
        final File basePath = folder.newFolder("cq");
        List<ProducerStatus> pret;
        List<ConsumerStatus> cret;
        // 1. Create a mq and run the producers
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath, NB_QUEUE)) {
            ProducerPool<IdMessage> producers = new ProducerPool<>(mQueues,
                    new RandomIdMessageProducerFactory(NB_DOCUMENTS), NB_PRODUCERS);
            pret = producers.call();
        }
        assertEquals(NB_PRODUCERS, pret.stream().count());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, pret.stream().mapToLong(r -> r.nbProcessed).sum());

        // 2. Use the mq and run the consumers
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath)) {
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mQueues,
                    new IdMessageFactory(IdMessageFactory.ConsumerType.NOOP), new ConsumerPolicy());
            cret = consumers.call();
        }
        assertEquals(NB_QUEUE, cret.stream().count());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, cret.stream().mapToLong(r -> r.committed).sum());
    }

    @Test
    public void producersAndConsumersConcurrently() throws Exception {
        final int NB_QUEUE = 10;
        final int NB_PRODUCERS = 15;
        final int NB_DOCUMENTS = 1000;
        final File basePath = folder.newFolder("cq");
        List<ProducerStatus> pret;
        List<ConsumerStatus> cret;
        // Create a mq
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath, NB_QUEUE)) {
            // execute producers and consumers concurrently
            ExecutorService es = Executors.newFixedThreadPool(2);
            ProducerPool<IdMessage> producers = new ProducerPool<>(mQueues,
                    new RandomIdMessageProducerFactory(NB_DOCUMENTS), NB_PRODUCERS);
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mQueues,
                    new IdMessageFactory(IdMessageFactory.ConsumerType.NOOP), new ConsumerPolicy());
            Future<List<ProducerStatus>> producersFuture = es.submit(producers);
            Future<List<ConsumerStatus>> consumersFuture = es.submit(consumers);
            // wait for the completion
            pret = producersFuture.get();
            cret = consumersFuture.get();
        }
        assertEquals(NB_PRODUCERS, pret.stream().count());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, pret.stream().mapToLong(r -> r.nbProcessed).sum());

        assertEquals(NB_QUEUE, cret.stream().count());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, cret.stream().mapToLong(r -> r.committed).sum());

    }

    @Test
    public void producerAndBuggyConsumers() throws Exception {
        final int NB_QUEUE = 23;
        // ordered message producer requires nb_producer <= nb consumer
        final int NB_PRODUCERS = NB_QUEUE;
        final int NB_DOCUMENTS = 10151;
        // final int NB_DOCUMENTS = 499999;
        final int BATCH_SIZE = 13;
        final File basePath = folder.newFolder("cq");
        List<ProducerStatus> pret;
        List<ConsumerStatus> cret;

        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath, NB_QUEUE)) {
            ProducerPool<IdMessage> producers = new ProducerPool<>(mQueues,
                    new RandomIdMessageProducerFactory(NB_DOCUMENTS, RandomIdMessageProducerFactory.ProducerType.ORDERED),
                    NB_PRODUCERS);
            pret = producers.call();
        }
        assertEquals(NB_PRODUCERS, pret.stream().count());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, pret.stream().mapToLong(r -> r.nbProcessed).sum());

        // 2. Use the mq and run the consumers
        try (MQueues<IdMessage> mQueues = new CQMQueues<>(basePath)) {
            ConsumerPool<IdMessage> consumers = new ConsumerPool<>(mQueues,
                    new IdMessageFactory(IdMessageFactory.ConsumerType.BUGGY),
                    new ConsumerPolicy().batchPolicy(new BatchPolicy().capacity(BATCH_SIZE))
                        .retryPolicy(new RetryPolicy().withMaxRetries(1000)));
            cret = consumers.call();
        }
        assertEquals(NB_QUEUE, cret.stream().count());
        assertEquals(NB_PRODUCERS * NB_DOCUMENTS, cret.stream().mapToLong(r -> r.committed).sum());
        assertTrue(NB_PRODUCERS * NB_DOCUMENTS < cret.stream().mapToLong(r -> r.accepted).sum());
    }


}

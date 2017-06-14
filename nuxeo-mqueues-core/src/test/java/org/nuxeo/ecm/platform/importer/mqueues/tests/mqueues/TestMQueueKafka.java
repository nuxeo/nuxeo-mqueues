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
package org.nuxeo.ecm.platform.importer.mqueues.tests.mqueues;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.stats.Total;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRebalanceException;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRebalanceListener;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRecord;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.management.RuntimeErrorException;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestMQueueKafka extends TestMQueue {
    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_PREFIX = "nuxeo-test";
    protected String prefix;

    @BeforeClass
    public static void assumeKafkaEnabled() {
        Assume.assumeTrue(KafkaUtils.kafkaDetected());
    }

    @Override
    public MQManager<IdMessage> createManager() throws Exception {
        if (prefix == null) {
            prefix = getPrefix();
        }
        return new KafkaMQManager<>(KafkaUtils.DEFAULT_ZK_SERVER, prefix, getProducerProps(), getConsumerProps());
    }

    @After
    public void resetPrefix() {
        prefix = null;
    }

    public static String getPrefix() {
        return TOPIC_PREFIX + "-" + System.currentTimeMillis() + "-";
    }

    public static Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        return props;
    }

    public static Properties getConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        // consumer are removed from a group if there more than this interval between poll
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 20000);
        // session timeout, consumer is removed from a group if there is no heartbeat on this interval
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        // short ht interval so that rebalance don't take for ever
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 400);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        return props;
    }

    @Test
    public void testSubscribe() throws Exception {
        final int NB_QUEUE = 3;
        final int NB_MSG = 1000;
        final int NB_CONSUMER = 20;
        final String group = "consumer";

        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);

        IdMessage msg1 = IdMessage.of("id1");
        IdMessage msg2 = IdMessage.of("id2");
        for (int i=0; i< NB_MSG; i++) {
            appender.append(i % NB_QUEUE, msg1);
        }

        MQTailer<IdMessage> tailer1 = manager.subscribe(group, Collections.singleton(mqName), null);

        // until we call read there is no assignments
        assertTrue(tailer1.assignments().isEmpty());
        MQRecord<IdMessage> record = null;
        try {
            tailer1.read(Duration.ofSeconds(2));
            fail("Should have raise a rebalance exception");
        } catch (MQRebalanceException e) {
            // expected
        }
        // assignments have been done with a single tailer
        assertFalse(tailer1.assignments().isEmpty());
        assertEquals(NB_QUEUE, tailer1.assignments().size());
        // read all the messages and commit
        for (int i=0; i< NB_QUEUE; i++) {
            record = tailer1.read(Duration.ofSeconds(1));
            assertNotNull(record);
            assertEquals(msg1, record.value());
        }
        tailer1.commit();
        tailer1.close();

        // And now enter the 2nd tailer
        Callable<Integer> consumer = () -> {
            int count = 0;
            MQTailer<IdMessage> consumerTailer = manager.subscribe(group, Collections.singleton(mqName), null);
            MQRecord<IdMessage> consumerRecord = null;
            while(true) {
                try {
                    consumerRecord = consumerTailer.read(Duration.ofMillis(200));
                    if (consumerRecord == null) {
                        return count;
                    }
                    count++;
                } catch (MQRebalanceException e) {
                    // expected, we start from last committed value
                    count = 0;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        };
        ExecutorService service = Executors.newFixedThreadPool(NB_CONSUMER);
        List<Future<Integer>> ret = new ArrayList<>(NB_CONSUMER);
        for (int i=0; i< NB_CONSUMER; i++) {
            ret.add(service.submit(consumer));
        }
        service.shutdown();
        service.awaitTermination(60, TimeUnit.SECONDS);
        int total = 0;
        for(Future<Integer> future: ret) {
            int count = future.get();
            System.out.println("Got: " + count);
            total += count;
        }
        assertEquals(NB_MSG - NB_QUEUE, total);
    }

}

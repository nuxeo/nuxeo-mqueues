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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.internals;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.Message;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerIterator;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerStatus;

import java.util.concurrent.Callable;

import static java.lang.Thread.currentThread;
import static org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.internals.ConsumerRunner.NUXEO_METRICS_REGISTRY_NAME;

/**
 * A callable pulling a producer iterator in loop.
 *
 * @since 9.1
 */
public class ProducerRunner<M extends Message> implements Callable<ProducerStatus> {
    private static final Log log = LogFactory.getLog(ProducerRunner.class);
    private final int producerId;
    private final MQueue<M> mq;
    private final ProducerFactory<M> factory;
    private String threadName;

    protected final MetricRegistry registry = SharedMetricRegistries.getOrCreate(NUXEO_METRICS_REGISTRY_NAME);
    protected final Timer producerTimer;
    protected final Counter producersCount;

    public ProducerRunner(ProducerFactory<M> factory, MQueue<M> mQueue, int producerId) {
        this.factory = factory;
        this.producerId = producerId;
        this.mq = mQueue;
        producerTimer = newTimer(MetricRegistry.name("nuxeo", "importer", "queue", "producer", String.valueOf(producerId)));
        producersCount = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "producers"));
        log.debug("ProducerIterator thread created: " + producerId);
    }

    private Counter newCounter(String name) {
        registry.remove(name);
        return registry.counter(name);
    }

    private Timer newTimer(String name) {
        registry.remove(name);
        return registry.timer(name);
    }

    @Override
    public ProducerStatus call() throws Exception {
        threadName = currentThread().getName();
        long start = System.currentTimeMillis();
        producersCount.inc();
        try (ProducerIterator<M> producer = factory.createProducer(producerId)) {
            producerLoop(producer);
        } finally {
            producersCount.dec();
        }
        return new ProducerStatus(producerId, producerTimer.getCount(), start, System.currentTimeMillis(), false);
    }

    private void producerLoop(ProducerIterator<M> producer) {
        M message;
        while (producer.hasNext()) {
            try (Timer.Context ignored = producerTimer.time()) {
                message = producer.next();
                setThreadName(message);
            }
            mq.append(producer.getShard(message, mq.size()), message);
        }
    }

    private void setThreadName(M message) {
        String name = threadName + "-" + producerTimer.getCount();
        if (message != null) {
            name += "-" + message.getId();
        } else {
            name += "-null";
        }
        currentThread().setName(name);
    }
}
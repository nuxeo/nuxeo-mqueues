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
package org.nuxeo.ecm.platform.importer.mqueues.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.runtime.metrics.MetricsService;

import java.util.concurrent.Callable;

import static java.lang.Thread.currentThread;

/**
 * A callable running a producer loop.
 *
 * @since 9.1
 */
public class ProducerCallable<M extends Message> implements Callable<ProducerStatus> {
    private static final Log log = LogFactory.getLog(ProducerCallable.class);
    private final int producerId;
    private final MQueues<M> mq;
    private final Producer<M> producer;
    private String threadName;

    protected final MetricRegistry registry = SharedMetricRegistries.getOrCreate(MetricsService.class.getName());
    protected final Timer producerTimer;
    protected final Counter producersCount;

    public ProducerCallable(ProducerFactory<M> factory, MQueues<M> mQueues, int producerId) {
        this.producer = factory.createProducer(producerId);
        this.producerId = producerId;
        this.mq = mQueues;
        producerTimer = newTimer(MetricRegistry.name("nuxeo", "importer", "queue", "producer", String.valueOf(producerId)));
        producersCount = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "producers"));

        log.debug("Producer thread created: " + producerId);
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
        producersCount.inc();
        threadName = currentThread().getName();
        long start = System.currentTimeMillis();
        try {
            producerLoop();
        } finally {
            producer.close();
            producersCount.dec();
        }
        return new ProducerStatus(producerId, producerTimer.getCount(), start, System.currentTimeMillis());
    }

    private void producerLoop() {
        M message;
        while (producer.hasNext()) {
            try (Timer.Context ignored = producerTimer.time()) {
                message = producer.next();
                setThreadName(message);
            }
            mq.put(producer.getShard(message, mq.size()), message);
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
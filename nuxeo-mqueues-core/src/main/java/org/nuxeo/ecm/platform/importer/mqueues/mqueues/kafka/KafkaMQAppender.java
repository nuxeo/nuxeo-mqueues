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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQOffsetImpl;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Apache Kafka implementation of MQueue.
 *
 * @since 9.2
 */
public class KafkaMQAppender<M extends Externalizable> implements MQAppender<M> {
    private static final Log log = LogFactory.getLog(KafkaMQAppender.class);
    private final String topic;
    private final Properties consumerProps;
    private final Properties producerProps;
    private final int size;
    private KafkaProducer<String, Bytes> producer;
    // keep track of created tailers to make sure they are closed
    private final ConcurrentLinkedQueue<KafkaMQTailer<M>> tailers = new ConcurrentLinkedQueue<>();
    private final String name;
    private boolean closed;

    static public <M extends Externalizable> KafkaMQAppender<M> open(String topic, String name, Properties producerProperties, Properties consumerProperties) {
        return new KafkaMQAppender<>(topic, name, producerProperties, consumerProperties);
    }

    private KafkaMQAppender(String topic, String name, Properties producerProperties, Properties consumerProperties) {
        this.topic = topic;
        this.name = name;
        this.producerProps = producerProperties;
        this.consumerProps = consumerProperties;
        this.producer = new KafkaProducer<>(this.producerProps);
        this.size = producer.partitionsFor(topic).size();
        log.debug(String.format("Created appender: %s on topic: %s with %d partitions", name, topic, size));
    }

    @Override
    public String name() {
        return name;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public MQOffset append(int partition, Externalizable message) {
        Bytes value = Bytes.wrap(messageAsByteArray(message));
        String key = String.valueOf(partition);
        ProducerRecord<String, Bytes> record = new ProducerRecord<>(topic, partition, key, value);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata result;
        try {
            result = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Unable to send record: " + record, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Unable to send record: " + record, e);
        }
        MQOffset ret = new MQOffsetImpl(name, partition, result.offset());
        if (log.isDebugEnabled()) {
            int len = record.value().get().length;
            log.debug(String.format("append to %s-%02d:+%d, len: %d, key: %s, value: %s", name,
                    partition, ret.offset(), len, key, message));
        }
        return ret;
    }

    private byte[] messageAsByteArray(Externalizable message) {
        ObjectOutput out;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            out = new ObjectOutputStream(bos);
            out.writeObject(message);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean waitFor(MQOffset offset, String group, Duration timeout) throws InterruptedException {
        boolean ret = false;
        if (!name.equals(offset.partition().name())) {
            throw new IllegalArgumentException(name + " can not wait for an offset of a different MQueue: " + offset);
        }
        TopicPartition topicPartition = new TopicPartition(topic, offset.partition().partition());
        try {
            ret = isProcessed(group, topicPartition, offset.offset());
            if (ret) {
                return true;
            }
            final long timeoutMs = timeout.toMillis();
            final long deadline = System.currentTimeMillis() + timeoutMs;
            final long delay = Math.min(100, timeoutMs);
            while (!ret && System.currentTimeMillis() < deadline) {
                Thread.sleep(delay);
                ret = isProcessed(group, topicPartition, offset.offset());
            }
            return ret;
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("waitFor " + offset + "/" + group + " returns: " + ret);
            }
        }
    }

    @Override
    public boolean closed() {
        return closed;
    }

    private boolean isProcessed(String group, TopicPartition topicPartition, long offset) {
        // TODO: find a better way, this is expensive to create a consumer each time
        // but this is needed, an open consumer is not properly updated
        Properties props = (Properties) consumerProps.clone();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        KafkaConsumer<String, Bytes> consumer = new KafkaConsumer<>(props);
        consumer.assign(Collections.singletonList(topicPartition));
        try {
            long last = consumer.position(topicPartition);
            boolean ret = (last > 0) && (last > offset);
            if (log.isDebugEnabled()) {
                log.debug("isProcessed " + topicPartition.topic() + ":" + topicPartition.partition() + "/" + group
                        + ":+" + offset + "? " + ret + ", current position: " + last);
            }
            return ret;
        } finally {
            consumer.close();
        }
    }


    @Override
    public void close() throws Exception {
        log.debug("Closing appender: " + name);
        tailers.stream().filter(Objects::nonNull).forEach(tailer -> {
            try {
                tailer.close();
            } catch (Exception e) {
                log.error("Failed to close tailer: " + tailer);
            }
        });
        tailers.clear();
        if (producer != null) {
            producer.close();
            producer = null;
        }
        closed = true;
    }
}

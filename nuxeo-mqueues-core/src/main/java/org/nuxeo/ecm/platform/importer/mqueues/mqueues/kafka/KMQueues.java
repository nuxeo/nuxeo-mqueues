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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.OffsetImpl;

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
 * Apache Kafka implementation of MQueues.
 *
 * @since 9.2
 */
public class KMQueues<M extends Externalizable> implements MQueues<M> {
    private static final Log log = LogFactory.getLog(KMQueues.class);
    private static final String DEFAULT_GROUP = "default";
    private final String topic;
    private final Properties consumerProps;
    private final Properties producerProps;
    private final int size;
    private KafkaProducer<String, Bytes> producer;
    // keep track of created tailers to make sure they are closed
    private final ConcurrentLinkedQueue<KQTailer<M>> tailers = new ConcurrentLinkedQueue<>();

    static public <M extends Externalizable> KMQueues<M> open(String topic, Properties producerProperties, Properties consumerProperties) {
        return new KMQueues<>(topic, producerProperties, consumerProperties);
    }

    private KMQueues(String topic, Properties producerProperties, Properties consumerProperties) {
        this.topic = topic;
        this.producerProps = normalizeProducerProperties(producerProperties);
        this.consumerProps = normalizeConsumerProperties(consumerProperties);
        this.producer = new KafkaProducer<>(this.producerProps);
        this.size = producer.partitionsFor(topic).size();
    }

    private Properties normalizeConsumerProperties(Properties consumerProperties) {
        Properties ret;
        if (consumerProperties != null) {
            ret = (Properties) consumerProperties.clone();
        } else {
            ret = new Properties();
        }
        ret.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        ret.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
        ret.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        ret.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return ret;
    }

    private Properties normalizeProducerProperties(Properties producerProperties) {
        Properties ret;
        if (producerProperties != null) {
            ret = (Properties) producerProperties.clone();
        } else {
            ret = new Properties();
        }
        ret.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        ret.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesSerializer");
        return ret;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Offset append(int queue, Externalizable message) {
        Bytes value = Bytes.wrap(messageAsByteArray(message));
        ProducerRecord<String, Bytes> record = new ProducerRecord<>(topic, queue, Integer.toString(queue), value);
        Future<RecordMetadata> result = producer.send(record);
        RecordMetadata ret;
        try {
            ret = result.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to send record: " + record, e);
        }
        if (log.isDebugEnabled()) {
            log.debug("append to " + topic + ":" + queue + ":+" + ret.offset() + ", msg: " + message);
        }
        return new OffsetImpl(queue, ret.offset());
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
    public Tailer<M> createTailer(int queue) {
        return createTailer(queue, DEFAULT_GROUP);
    }

    @Override
    public Tailer<M> createTailer(int queue, String name) {
        Properties props = (Properties) consumerProps.clone();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, name);
        KafkaConsumer<String, Bytes> consumer = new KafkaConsumer<>(props);
        KQTailer<M> ret = new KQTailer<>(consumer, new TopicPartition(topic, queue), name);
        tailers.add(ret);
        return ret;
    }

    @Override
    public boolean waitFor(Offset offset, Duration timeout) throws InterruptedException {
        return waitFor(offset, DEFAULT_GROUP, timeout);
    }

    @Override
    public boolean waitFor(Offset offset, String nameSpace, Duration timeout) throws InterruptedException {
        boolean ret = false;
        long offsetPosition = ((OffsetImpl) offset).getOffset();
        int partition = ((OffsetImpl) offset).getQueue();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Properties props = (Properties) consumerProps.clone();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, nameSpace);
        KafkaConsumer<String, Bytes> consumer = new KafkaConsumer<>(props);
        try {
            consumer.assign(Collections.singletonList(topicPartition));
            ret = isProcessed(consumer, nameSpace, topicPartition, offsetPosition);
            if (ret) {
                return true;
            }
            final long timeoutMs = timeout.toMillis();
            final long deadline = System.currentTimeMillis() + timeoutMs;
            final long delay = Math.min(100, timeoutMs);
            while (!ret && System.currentTimeMillis() < deadline) {
                Thread.sleep(delay);
                ret = isProcessed(consumer, nameSpace, topicPartition, offsetPosition);
            }
            return ret;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            if (log.isDebugEnabled()) {
                log.debug("waitFor " + topicPartition.topic() + ":" + topicPartition.partition() + "/" + nameSpace
                        + ":+" + offsetPosition + " returns: " + ret);
            }
        }
    }

    private boolean isProcessed(KafkaConsumer<String, Bytes> consumer, String group, TopicPartition topicPartition, long offset) {
        long last = consumer.position(topicPartition);
        boolean ret = (last > 0) && (last > offset);
        if (log.isDebugEnabled()) {
            log.debug("isProcessed " + topicPartition.topic() + ":" + topicPartition.partition() + "/" + group
                    + ":+" + offset + "? " + ret + ", current position: " + last);
        }
        return ret;
    }

    @Override
    public void close() throws Exception {
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
    }
}

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

import kafka.cluster.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRecord;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQOffsetImpl;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQPartitionGroup;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @since 9.2
 */
public class KafkaMQTailer<M extends Externalizable> implements MQTailer<M> {
    private static final Log log = LogFactory.getLog(KafkaMQTailer.class);
    private final MQPartitionGroup id;
    private final TopicPartition topicPartition;
    private KafkaConsumer<String, Bytes> consumer;
    private long lastOffset = 0;
    private long lastCommittedOffset = 0;
    private final Queue<ConsumerRecord<String, Bytes>> records = new LinkedList<>();
    // keep track of all tailers on the same namespace index even from different mq
    private static final Set<MQPartitionGroup> tailersId = Collections.newSetFromMap(new ConcurrentHashMap<MQPartitionGroup, Boolean>());
    private boolean closed = false;

    public KafkaMQTailer(String topic, MQPartition partition, String group, Properties consumerProps) {
        Objects.requireNonNull(group);
        this.id = new MQPartitionGroup(group, partition.name(), partition.partition());
        this.topicPartition = new TopicPartition(topic, partition.partition());

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.assign(Collections.singletonList(topicPartition));
        OffsetAndMetadata offsetMeta = consumer.committed(topicPartition);
        if (offsetMeta != null) {
            this.lastCommittedOffset = offsetMeta.offset();
        } else {
            this.lastCommittedOffset = consumer.position(topicPartition);
        }
        registerTailer();
        log.debug("Create tailer " + id + ":+" + lastCommittedOffset);
    }

    @Override
    public MQRecord<M> read(Duration timeout) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("The tailer has been closed.");
        }
        if (records.isEmpty()) {
            if (poll(timeout) == 0) {
                if (log.isTraceEnabled()) {
                    log.trace("No data " + id + " after " + timeout.toMillis() + " ms");
                }
                return null;
            }
        }
        ConsumerRecord<String, Bytes> record = records.poll();
        lastOffset = record.offset();
        M value = messageOf(record.value());
        if (log.isDebugEnabled()) {
            log.debug("Read " + id + ":+" + record.offset() + " returns key: "
                    + record.key() + ", msg: " + value);
        }
        return new MQRecord(new MQPartition(id.name, record.partition()), value,
                new MQOffsetImpl(record.partition(), record.offset()));
    }

    @SuppressWarnings("unchecked")
    private M messageOf(Bytes value) {
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            return (M) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    private int poll(Duration timeout) throws InterruptedException {
        records.clear();
        try {
            for (ConsumerRecord<String, Bytes> record : consumer.poll(timeout.toMillis())) {
                records.add(record);
            }
        } catch (org.apache.kafka.common.errors.InterruptException e) {
            Thread.interrupted();
            throw new InterruptedException(e.getMessage());
        }
        if (log.isDebugEnabled()) {
            String msg = "Polling " + id + " returns " + records.size() + " records";
            if (records.size() > 0) {
                log.debug(msg);
            } else {
                log.trace(msg);
            }
        }
        return records.size();
    }

    @Override
    public void toEnd() {
        log.debug("toEnd: " + id);
        lastOffset = 0;
        records.clear();
        consumer.seekToEnd(Collections.singleton(topicPartition));
    }

    @Override
    public void toStart() {
        log.debug("toStart: " + id);
        lastOffset = 0;
        records.clear();
        consumer.seekToBeginning(Collections.singleton(topicPartition));
    }

    @Override
    public void toLastCommitted() {
        log.debug("toLastCommitted: " + id + ":+" + lastCommittedOffset);
        consumer.seek(topicPartition, lastCommittedOffset);
        records.clear();
    }

    @Override
    public MQOffset commit() {
        lastCommittedOffset = lastOffset + 1;
        consumer.commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(lastCommittedOffset)));
        if (log.isDebugEnabled()) {
            log.debug("Commit position: " + id + ":+" + lastCommittedOffset);
        }
        return new MQOffsetImpl(topicPartition.partition(), lastOffset);
    }

    @Override
    public int getQueue() {
        return topicPartition.partition();
    }

    @Override
    public MQPartition getMQPartition() {
        return new MQPartition(id.name, id.partition);
    }

    @Override
    public String getGroup() {
        return id.group;
    }

    @Override
    public boolean closed() {
        return closed;
    }

    @Override
    public void close() throws Exception {
        unregisterTailer();
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        closed = true;
    }

    private void registerTailer() {
        if (!tailersId.add(id)) {
            throw new IllegalArgumentException("A tailer for this queue and namespace already exists: " + id);
        }
    }

    private void unregisterTailer() {
        tailersId.remove(id);
    }

}

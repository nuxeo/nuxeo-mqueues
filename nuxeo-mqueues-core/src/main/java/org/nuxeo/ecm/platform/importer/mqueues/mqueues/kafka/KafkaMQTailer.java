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

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.stream.Collectors;


/**
 * @since 9.2
 */
public class KafkaMQTailer<M extends Externalizable> implements MQTailer<M> {
    private static final Log log = LogFactory.getLog(KafkaMQTailer.class);
    private final String id;
    private final Collection<TopicPartition> topicPartitions;
    private final String group;
    private final Collection<MQPartition> partitions;
    private final String prefix;
    private KafkaConsumer<String, Bytes> consumer;
    private final Map<TopicPartition, Long> lastOffsets = new HashMap<>();
    private final Map<TopicPartition, Long> lastCommittedOffsets = new HashMap<>();
    private final Queue<ConsumerRecord<String, Bytes>> records = new LinkedList<>();
    // keep track of all tailers on the same namespace index even from different mq
    private boolean closed = false;

    public KafkaMQTailer(String prefix, Collection<MQPartition> partitions, String group, Properties consumerProps) {
        Objects.requireNonNull(group);
        this.id = buildId(group, partitions);
        this.prefix = prefix;
        this.group = group;
        this.partitions = partitions;
        this.topicPartitions = partitions.stream().map(partition -> new TopicPartition(prefix + partition.name(),
                partition.partition())).collect(Collectors.toList());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.assign(topicPartitions);
        log.debug(String.format("Created tailer: %s using prefix: %s", id, prefix));
        toLastCommitted();
    }

    private String buildId(String group, Collection<MQPartition> partitions) {
        return group + ":" + partitions.stream().map(MQPartition::toString).collect(Collectors.joining("|"));
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
        lastOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
        M value = messageOf(record.value());
        if (log.isDebugEnabled()) {
            log.debug(String.format("Read from %s-%02d:+%d, key: %s, value: %s",
                    record.topic().replace(prefix, ""), record.partition(), record.offset(),
                    record.key(), value));
        }
        return new MQRecord<>(new MQPartition(getNameForTopic(record.topic()), record.partition()), value,
                new MQOffsetImpl(record.partition(), record.offset()));
    }

    private String getNameForTopic(String topic) {
        return topic.replaceFirst(prefix, "");
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
        lastOffsets.clear();
        records.clear();
        consumer.seekToEnd(topicPartitions);
    }

    @Override
    public void toStart() {
        log.debug("toStart: " + id);
        lastOffsets.clear();
        records.clear();
        consumer.seekToBeginning(topicPartitions);
    }

    @Override
    public void toLastCommitted() {
        log.debug("toLastCommitted tailer: " + id);
        topicPartitions.forEach(this::toLastCommitted);
        records.clear();
    }

    private void toLastCommitted(TopicPartition topicPartition) {
        Long offset = lastCommittedOffsets.get(topicPartition);
        if (offset == null) {
            OffsetAndMetadata offsetMeta = consumer.committed(topicPartition);
            if (offsetMeta != null) {
                offset = offsetMeta.offset();
            }
        }
        if (offset != null) {
            consumer.seek(topicPartition, offset);
        } else {
            consumer.seekToBeginning(Collections.singletonList(topicPartition));
            offset = consumer.position(topicPartition);
        }
        log.debug(String.format(" toLastCommitted: %s-%02d:+%d", topicPartition.topic().replace(prefix, ""),
                topicPartition.partition(),
                offset));
    }

    @Override
    public void commit() {
        Map<TopicPartition, OffsetAndMetadata> offsetToCommit = new HashMap<>();
        partitions.stream().map(partition -> new TopicPartition(prefix + partition.name(),
                partition.partition())).filter(lastOffsets::containsKey)
                .forEach(tp -> offsetToCommit.put(tp, new OffsetAndMetadata(lastOffsets.get(tp) + 1)));
        lastOffsets.clear();
        consumer.commitSync(offsetToCommit);
        offsetToCommit.forEach((topicPartition,  offset) -> lastCommittedOffsets.put(topicPartition, offset.offset()));
        if (log.isDebugEnabled()) {
            log.debug("Committed all positions for: " + id);
            offsetToCommit.forEach((tp, offset) -> log.debug(String.format(" Committed: %s:%s-%02d:+%d",
                    group, tp.topic().replace(prefix, ""), tp.partition(), offset.offset() + 1)));
        }
    }

    @Override
    public MQOffset commit(MQPartition partition) {
        TopicPartition topicPartition = new TopicPartition(prefix + partition.name(), partition.partition());
        Long offset = lastOffsets.get(topicPartition);
        if (offset == null) {
            throw new IllegalArgumentException("Can not commit unchanged partition: " + partition);
        }
        offset += 1;
        consumer.commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(offset)));
        if (log.isDebugEnabled()) {
            log.debug("Committed: " + partition.name() + ":" + partition.partition() + ":+" + offset);
        }
        return new MQOffsetImpl(topicPartition.partition(), (offset));
    }

    @Override
    public Collection<MQPartition> getMQPartitions() {
        return partitions;
    }

    @Override
    public String getGroup() {
        return group;
    }

    @Override
    public boolean closed() {
        return closed;
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            log.debug("Closing tailer: " + id);
            try {
                // calling wakeup enable to terminate consumer blocking on poll call
                consumer.wakeup();
                consumer.close();
            } catch (IllegalStateException|ConcurrentModificationException e) {
                // this happens if the consumer has already been closed or if it is closed from another
                // thread.
                log.warn("Discard error while closing consumer: ", e);
            }
            consumer = null;
        }
        closed = true;
    }

    @Override
    public String toString() {
        return "KafkaMQTailer{" +
                "prefix='" + prefix + '\'' +
                ", id=" + id +
                ", closed=" + closed +
                '}';
    }

}

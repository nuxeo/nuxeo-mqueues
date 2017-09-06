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
package org.nuxeo.lib.core.mqueues.mqueues.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Bytes;
import org.nuxeo.lib.core.mqueues.mqueues.MQOffset;
import org.nuxeo.lib.core.mqueues.mqueues.MQPartition;
import org.nuxeo.lib.core.mqueues.mqueues.MQRebalanceException;
import org.nuxeo.lib.core.mqueues.mqueues.MQRebalanceListener;
import org.nuxeo.lib.core.mqueues.mqueues.MQRecord;
import org.nuxeo.lib.core.mqueues.mqueues.MQTailer;
import org.nuxeo.lib.core.mqueues.mqueues.internals.MQOffsetImpl;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.stream.Collectors;


/**
 * @since 9.2
 */
public class KafkaMQTailer<M extends Externalizable> implements MQTailer<M>, ConsumerRebalanceListener {
    private static final Log log = LogFactory.getLog(KafkaMQTailer.class);
    protected final String group;
    protected final String prefix;
    protected KafkaConsumer<String, Bytes> consumer;
    protected String id;
    protected Collection<TopicPartition> topicPartitions;
    protected Collection<MQPartition> partitions;
    protected final Map<TopicPartition, Long> lastOffsets = new HashMap<>();
    protected final Map<TopicPartition, Long> lastCommittedOffsets = new HashMap<>();
    protected final Queue<ConsumerRecord<String, Bytes>> records = new LinkedList<>();
    // keep track of all tailers on the same namespace index even from different mq
    protected boolean closed = false;
    protected Collection<String> names;
    protected MQRebalanceListener listener;
    protected boolean isRebalanced = false;

    protected KafkaMQTailer(String prefix, String group, Properties consumerProps) {
        Objects.requireNonNull(group);
        this.prefix = prefix;
        this.group = group;
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        this.consumer = new KafkaConsumer<>(consumerProps);

    }

    public static <M extends Externalizable> KafkaMQTailer<M> createAndAssign(String prefix, Collection<MQPartition> partitions, String group, Properties consumerProps) {
        KafkaMQTailer<M> ret = new KafkaMQTailer<>(prefix, group, consumerProps);
        ret.id = buildId(ret.group, partitions);
        ret.partitions = partitions;
        ret.topicPartitions = partitions.stream().map(partition -> new TopicPartition(prefix + partition.name(),
                partition.partition())).collect(Collectors.toList());
        ret.consumer.assign(ret.topicPartitions);
        log.debug(String.format("Created tailer with assignments: %s using prefix: %s", ret.id, prefix));
        return ret;
    }

    public static <M extends Externalizable> KafkaMQTailer<M> createAndSubscribe(String prefix, Collection<String> names, String group, Properties consumerProps,
                                                                                 MQRebalanceListener listener) {
        KafkaMQTailer<M> ret = new KafkaMQTailer<>(prefix, group, consumerProps);
        ret.id = buildSubscribeId(ret.group, names);
        ret.names = names;
        Collection<String> topics = names.stream().map(name -> prefix + name).collect(Collectors.toList());
        ret.listener = listener;
        ret.consumer.subscribe(topics, ret);
        ret.partitions = Collections.emptyList();
        ret.topicPartitions = Collections.emptyList();
        log.debug(String.format("Created tailer with subscription: %s using prefix: %s", ret.id, prefix));
        return ret;
    }

    protected static String buildId(String group, Collection<MQPartition> partitions) {
        return group + ":" + partitions.stream().map(MQPartition::toString).collect(Collectors.joining("|"));
    }

    protected static String buildSubscribeId(String group, Collection<String> names) {
        return group + ":" + names.stream().collect(Collectors.joining("|"));
    }


    @Override
    public MQRecord<M> read(Duration timeout) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("The tailer has been closed.");
        }
        if (records.isEmpty()) {
            int items = poll(timeout);
            if (isRebalanced) {
                isRebalanced = false;
                log.debug("Rebalance happens during poll, raising exception");
                throw new MQRebalanceException("Partitions has been rebalanced");
            }
            if (items == 0) {
                if (log.isTraceEnabled()) {
                    log.trace("No data " + id + " after " + timeout.toMillis() + " ms");
                }
                return null;
            }
        }
        ConsumerRecord<String, Bytes> record = records.poll();
        lastOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
        M value = messageOf(record.value());
        MQPartition partition = MQPartition.of(getNameForTopic(record.topic()), record.partition());
        MQOffset offset = new MQOffsetImpl(partition, record.offset());
        if (log.isDebugEnabled()) {
            log.debug(String.format("Read from %s/%s, key: %s, value: %s", offset, group, record.key(), value));
        }
        return new MQRecord<>(partition, value, offset);
    }

    protected String getNameForTopic(String topic) {
        return topic.replaceFirst(prefix, "");
    }

    @SuppressWarnings("unchecked")
    protected M messageOf(Bytes value) {
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

    protected int poll(Duration timeout) throws InterruptedException {
        records.clear();
        try {
            for (ConsumerRecord<String, Bytes> record : consumer.poll(timeout.toMillis())) {
                if (log.isDebugEnabled() && records.isEmpty()) {
                    log.debug("Poll first record: " + getNameForTopic(record.topic()) + ":" + record.partition() + ":+" + record.offset());
                }
                records.add(record);
            }
        } catch (org.apache.kafka.common.errors.InterruptException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException(e.getMessage());
        } catch (WakeupException e) {
            log.debug("Receiving wakeup from another thread to close the tailer");
            try {
                close();
            } catch (Exception e1) {
                log.warn("Error while closing the tailer " + this);
            }
            throw new IllegalStateException("poll interrupted because tailer has been closed");
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
        consumer.seekToEnd(Collections.emptyList());
    }

    @Override
    public void toStart() {
        log.debug("toStart: " + id);
        lastOffsets.clear();
        records.clear();
        consumer.seekToBeginning(Collections.emptyList());
    }

    @Override
    public void toLastCommitted() {
        log.debug("toLastCommitted tailer: " + id);
        String msg = consumer.assignment().stream().map(tp -> String.format("%s-%02d:+%d",
                getNameForTopic(tp.topic()), tp.partition(), toLastCommitted(tp)))
                .collect(Collectors.joining("|"));
        if (msg.length() > 0) {
            log.info("toLastCommitted offsets: " + group + ":" + msg);
        }
        lastCommittedOffsets.clear();
        lastOffsets.clear();
        records.clear();
    }

    protected long toLastCommitted(TopicPartition topicPartition) {
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
        log.debug(String.format(" toLastCommitted: %s-%02d:+%d", getNameForTopic(topicPartition.topic()),
                topicPartition.partition(),
                offset));
        return offset;
    }

    @Override
    public void seek(MQOffset offset) {
        log.debug("Seek to: " + offset + " from tailer: " + id);
        TopicPartition topicPartition = new TopicPartition(prefix + offset.partition().name(),
                offset.partition().partition());
        consumer.seek(topicPartition, offset.offset());
        lastOffsets.remove(topicPartition);
        List<ConsumerRecord<String, Bytes>> toRemove = records.stream().filter(rec -> rec.partition() == topicPartition.partition()).collect(Collectors.toList());
        toRemove.forEach(records::remove);
    }

    @Override
    public void reset() {
        // we just commit the first offset
        log.info("Reset committed offsets for all assigned partitions: " + topicPartitions + " tailer: " + id);
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
        Map<TopicPartition, OffsetAndMetadata> offsetToCommit = new HashMap<>();
        beginningOffsets.forEach((tp, offset) -> offsetToCommit.put(tp, new OffsetAndMetadata(offset)));
        consumer.commitSync(offsetToCommit);
        lastCommittedOffsets.clear();
        toLastCommitted();
    }

    @Override
    public void reset(MQPartition partition) {
        log.info("Reset committed offset for partition: " + partition + " tailer: " + id);
        TopicPartition topicPartition = new TopicPartition(prefix + partition.name(), partition.partition());
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singleton(topicPartition));
        Map<TopicPartition, OffsetAndMetadata> offsetToCommit = new HashMap<>();
        beginningOffsets.forEach((tp, offset) -> offsetToCommit.put(tp, new OffsetAndMetadata(offset)));
        consumer.commitSync(offsetToCommit);
        lastCommittedOffsets.remove(topicPartition);
        seek(new MQOffsetImpl(partition, beginningOffsets.get(0)));
    }

    @Override
    public void commit() {
        Map<TopicPartition, OffsetAndMetadata> offsetToCommit = new HashMap<>();
        lastOffsets.forEach((tp, offset) -> offsetToCommit.put(tp, new OffsetAndMetadata(offset + 1)));
        lastOffsets.clear();
        if (offsetToCommit.isEmpty()) {
            return;
        }
        consumer.commitSync(offsetToCommit);
        offsetToCommit.forEach((topicPartition, offset) -> lastCommittedOffsets.put(topicPartition, offset.offset()));
        if (log.isDebugEnabled()) {
            String msg = offsetToCommit.entrySet().stream().map(entry -> String.format("%s-%02d:+%d",
                    getNameForTopic(entry.getKey().topic()), entry.getKey().partition(), entry.getValue().offset()))
                    .collect(Collectors.joining("|"));
            log.debug("Committed offsets  " + group + ":" + msg);
        }
    }

    @Override
    public MQOffset commit(MQPartition partition) {
        TopicPartition topicPartition = new TopicPartition(prefix + partition.name(), partition.partition());
        Long offset = lastOffsets.get(topicPartition);
        if (offset == null) {
            log.debug("unchanged partition, nothing to commit: " + partition);
            return null;
        }
        offset += 1;
        consumer.commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(offset)));
        MQOffset ret = new MQOffsetImpl(partition, offset);
        if (log.isDebugEnabled()) {
            log.info("Committed: " + offset + "/" + group);
        }
        return ret;
    }

    @Override
    public Collection<MQPartition> assignments() {
        return partitions;
    }

    @Override
    public String group() {
        return group;
    }

    @Override
    public boolean closed() {
        return closed;
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            log.info("Closing tailer: " + id);
            try {
                // calling wakeup enable to terminate consumer blocking on poll call
                consumer.close();
            } catch (ConcurrentModificationException e) {
                // closing from another thread raise this exception, try to wakeup the owner
                log.info("Closing tailer from another thread, send wakeup");
                consumer.wakeup();
                return;
            } catch (InterruptException | IllegalStateException e) {
                // this happens if the consumer has already been closed or if it is closed from another
                // thread.
                log.warn("Discard error while closing consumer: ", e);
            } catch (Throwable t) {
                log.error("interrupted", t);
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

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Collection<MQPartition> revoked = partitions.stream()
                .map(tp -> MQPartition.of(getNameForTopic(tp.topic()), tp.partition()))
                .collect(Collectors.toList());
        log.info(String.format("Rebalance revoked: %s", revoked));
        id += "-revoked";
        if (listener != null) {
            listener.onPartitionsRevoked(revoked);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> newPartitions) {
        partitions = newPartitions.stream().map(tp -> MQPartition.of(getNameForTopic(tp.topic()), tp.partition()))
                .collect(Collectors.toList());
        topicPartitions = newPartitions;
        id = buildId(group, partitions);
        lastCommittedOffsets.clear();
        lastOffsets.clear();
        records.clear();
        isRebalanced = true;
        log.info(String.format("Rebalance assigned: %s", partitions));
        if (listener != null) {
            listener.onPartitionsAssigned(partitions);
        }
    }

}

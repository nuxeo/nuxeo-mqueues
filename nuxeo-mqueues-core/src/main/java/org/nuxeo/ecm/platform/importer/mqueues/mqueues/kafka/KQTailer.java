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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.OffsetImpl;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @since 9.2
 */
public class KQTailer<M extends Externalizable> implements MQueues.Tailer<M> {
    private static final Log log = LogFactory.getLog(KQTailer.class);
    private final String nameSpace;
    private KafkaConsumer<String, Bytes> consumer;
    private final TopicPartition topicPartition;
    private long lastOffset = 0;
    private long lastCommittedOffset = 0;
    private final Queue<ConsumerRecord<String, Bytes>> records = new LinkedList<>();
    // keep track of all tailers on the same namespace index even from different mq
    private static final Set<String> indexNamespace = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public KQTailer(KafkaConsumer<String, Bytes> consumer, TopicPartition topicPartition, String nameSpace) {
        this.consumer = consumer;
        this.nameSpace = nameSpace;
        this.topicPartition = topicPartition;
        consumer.assign(Collections.singletonList(topicPartition));
        this.lastCommittedOffset = consumer.position(topicPartition);
        registerTailer();
        log.debug("Create tailer " + getTailerKey() + ":+" + lastCommittedOffset);
    }

    @Override
    public M read(Duration timeout) throws InterruptedException {
        if (records.isEmpty()) {
            if (poll(timeout) == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("No data " + getTailerKey() + " after " + timeout.toMillis() + " ms");
                }
                return null;
            }
        }
        ConsumerRecord<String, Bytes> record = records.poll();
        lastOffset = record.offset();
        M ret = messageOf(record.value());
        if (log.isDebugEnabled()) {
            log.debug("Read " + getTailerKey() + ":+" + record.offset() + " returns key: "
                    + record.key() + ", msg: " + ret);
        }
        return ret;
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
        log.debug("Polling " + getTailerKey() + " returns " + records.size() + " records");
        return records.size();
    }

    @Override
    public void toEnd() {
        log.debug("toEnd " + getTailerKey());
        lastOffset = 0;
        records.clear();
        consumer.seekToEnd(Collections.singleton(topicPartition));
    }

    @Override
    public void toStart() {
        log.debug("toStart " + getTailerKey());
        lastOffset = 0;
        records.clear();
        consumer.seekToBeginning(Collections.singleton(topicPartition));
    }

    @Override
    public void toLastCommitted() {
        log.debug("toLastCommitted " + getTailerKey() + ":+" + lastCommittedOffset);
        consumer.seek(topicPartition, lastCommittedOffset);
        records.clear();
    }

    @Override
    public Offset commit() {
        lastCommittedOffset = lastOffset + 1;
        consumer.commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(lastCommittedOffset)));
        if (log.isDebugEnabled()) {
            log.debug("Commit partition: " + getTailerKey() + ":+" + lastOffset);
        }
        return new OffsetImpl(topicPartition.partition(), lastOffset);
    }

    @Override
    public int getQueue() {
        return topicPartition.partition();
    }

    @Override
    public void close() throws Exception {
        unregisterTailer();
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    private void registerTailer() {
        String key = getTailerKey();
        if (!indexNamespace.add(key)) {
            throw new IllegalArgumentException("A tailer for this queue and namespace already exists: " + key);
        }
    }

    private void unregisterTailer() {
        String key = getTailerKey();
        indexNamespace.remove(key);
    }

    private String getTailerKey() {
        return topicPartition.topic() + ":" + topicPartition.partition() + "/" + nameSpace;
    }

}

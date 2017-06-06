package org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle;/*
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

import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRecord;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQOffsetImpl;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQPartitionGroup;

import java.io.Externalizable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @since 9.1
 */
public class ChronicleMQTailer<M extends Externalizable> implements MQTailer<M> {
    private static final Log log = LogFactory.getLog(ChronicleMQTailer.class);
    protected static final long POLL_INTERVAL_MS = 100L;

    private final String basePath;
    private final ExcerptTailer cqTailer;
    private final ChronicleMQOffsetTracker offsetTracker;
    private final MQPartitionGroup id;
    private final MQPartition partition;
    private boolean closed = false;

    // keep track of all tailers on the same namespace index even from different mq
    private static final Set<MQPartitionGroup> tailersId = Collections.newSetFromMap(new ConcurrentHashMap<MQPartitionGroup, Boolean>());

    public ChronicleMQTailer(String basePath, ExcerptTailer cqTailer, MQPartition partition, String group) {
        Objects.requireNonNull(group);
        this.basePath = basePath;
        this.cqTailer = cqTailer;
        this.partition = partition;
        this.id = new MQPartitionGroup(group, partition.name(), partition.partition());
        registerTailer();
        this.offsetTracker = new ChronicleMQOffsetTracker(basePath, partition.partition(), group);
        toLastCommitted();
    }

    private void registerTailer() {
        if (!tailersId.add(id)) {
            throw new IllegalArgumentException("A tailer for this queue and namespace already exists: " + id);
        }
    }

    private void unregisterTailer() {
        tailersId.remove(id);
    }

    @Override
    public MQRecord<M> read(Duration timeout) throws InterruptedException {
        MQRecord<M> ret = read();
        if (ret != null) {
            return ret;
        }
        final long timeoutMs = timeout.toMillis();
        final long deadline = System.currentTimeMillis() + timeoutMs;
        final long delay = Math.min(POLL_INTERVAL_MS, timeoutMs);
        while (ret == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(delay);
            ret = read();
        }
        return ret;
    }


    @SuppressWarnings("unchecked")
    protected MQRecord<M> read() {
        if (closed) {
            throw new IllegalStateException("The tailer has been closed.");
        }
        final List<M> value = new ArrayList<>(1);
        if (!cqTailer.readDocument(w -> value.add((M) w.read("msg").object()))) {
            return null;

        }
        MQRecord<M> ret = new MQRecord<>(new MQPartition(id.name, id.partition), value.get(0),
                new MQOffsetImpl(id.partition, cqTailer.index()));
        return ret;
    }

    @Override
    public MQOffset commit(MQPartition partition) {
        // we write raw: queue, offset, timestamp
        if (!this.partition.equals(partition)) {
            throw new IllegalArgumentException("Can not commit this partition: " + partition + " from " + id);
        }
        long offset = cqTailer.index();
        offsetTracker.commit(offset);
        if (log.isTraceEnabled()) {
            log.trace(String.format("Commit %s:+%d", id, offset));
        }
        return new MQOffsetImpl(id.partition, offset);
    }

    @Override
    public void commit() {
        commit(partition);
    }

    @Override
    public void toEnd() {
        log.debug(String.format("toEnd: %s", id));
        cqTailer.toEnd();
    }

    @Override
    public void toStart() {
        log.debug(String.format("toStart: %s", id));
        cqTailer.toStart();
    }

    @Override
    public void toLastCommitted() {
        long offset = offsetTracker.getLastCommittedOffset();
        if (offset > 0) {
            log.debug(String.format("toLastCommitted: %s, found: %d", id, offset));
            cqTailer.moveToIndex(offset);
        } else {
            log.debug(String.format("toLastCommitted: %s not found, run from beginning", id));
            cqTailer.toStart();
        }
    }

    @Override
    public Collection<MQPartition> getMQPartitions() {
        return Collections.singletonList(new MQPartition(id.name, id.partition));
    }

    @Override
    public String getGroup() {
        return id.group;
    }

    @Override
    public void close() throws Exception {
        offsetTracker.close();
        unregisterTailer();
        closed = true;
    }

    @Override
    public boolean closed() {
        return closed;
    }

    @Override
    public String toString() {
        return "ChronicleMQTailer{" +
                "basePath='" + basePath + '\'' +
                ", id=" + id +
                ", closed=" + closed +
                '}';
    }

}

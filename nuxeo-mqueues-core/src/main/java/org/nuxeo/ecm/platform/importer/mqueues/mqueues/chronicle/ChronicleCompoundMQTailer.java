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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle;

import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRecord;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;

import java.io.Externalizable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * @since 9.2
 */
public class ChronicleCompoundMQTailer<M extends Externalizable> implements MQTailer<M> {
    private final Collection<ChronicleMQTailer<M>> tailers;
    private final String group;
    private final List<MQPartition> mqPartitions;
    private boolean closed = false;
    private long counter = 0;

    public ChronicleCompoundMQTailer(Collection<ChronicleMQTailer<M>> tailers, String group) {
        if (tailers.isEmpty()) {
            throw new IllegalArgumentException("Can not create an empty compound tailer");
        }
        this.tailers = tailers;
        this.mqPartitions = tailers.stream().map(partition -> partition.getMQPartitions().stream().findFirst().get()).collect(Collectors.toList());
        this.group = group;
    }

    @Override
    public MQRecord<M> read(Duration timeout) throws InterruptedException {
        MQRecord<M> ret = read();
        if (ret != null) {
            return ret;
        }
        final long timeoutMs = timeout.toMillis();
        final long deadline = System.currentTimeMillis() + timeoutMs;
        final long delay = Math.min(ChronicleMQTailer.POLL_INTERVAL_MS, timeoutMs);
        while (ret == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(delay);
            ret = read();
        }
        return ret;
    }

    private MQRecord<M> read() {
        // round robin on tailers
        counter++;
        int toSkip = (int) (counter % tailers.size());
        int i = 0;
        MQRecord<M> ret;
        for (ChronicleMQTailer<M> tailer: tailers) {
            if (i < toSkip) {
                continue;
            }
            ret = tailer.read();
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }

    @Override
    public MQOffset commit(MQPartition partition) {
        return (MQOffset) tailers.stream().filter(tailer -> partition.equals(tailer.getMQPartitions())).findFirst().get();
    }

    @Override
    public void commit() {
        tailers.forEach(MQTailer::commit);
    }

    @Override
    public void toEnd() {
        tailers.forEach(ChronicleMQTailer::toEnd);
    }

    @Override
    public void toStart() {
        tailers.forEach(ChronicleMQTailer::toStart);
    }

    @Override
    public void toLastCommitted() {
        tailers.forEach(ChronicleMQTailer::toLastCommitted);
    }

    @Override
    public Collection<MQPartition> getMQPartitions() {
        return mqPartitions;
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
        for (ChronicleMQTailer<M> tailer : tailers) {
            tailer.close();
        }
        closed = true;
    }
}

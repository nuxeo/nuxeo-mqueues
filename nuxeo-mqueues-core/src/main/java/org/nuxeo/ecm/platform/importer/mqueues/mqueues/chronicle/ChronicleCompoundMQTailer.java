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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * @since 9.2
 */
public class ChronicleCompoundMQTailer<M extends Externalizable> implements MQTailer<M> {
    private final Collection<ChronicleMQTailer<M>> tailers;
    private final String group;
    private final int size;
    private final List<MQPartition> mqPartitions = new ArrayList<>();
    private boolean closed = false;
    private long counter = 0;

    public ChronicleCompoundMQTailer(Collection<ChronicleMQTailer<M>> tailers, String group) {
        // empty tailers is an accepted input
        this.tailers = tailers;
        this.group = group;
        this.size = tailers.size();
        tailers.stream().forEach(partition -> mqPartitions.addAll(partition.assignments()));
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
        int toSkip = (size > 0) ? ((int) (counter++ % size)) : 0;
        int i = 0;
        MQRecord<M> ret;
        for (ChronicleMQTailer<M> tailer : tailers) {
            if (i++ < toSkip) {
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
        for (MQTailer<M> tailer : tailers) {
            if (tailer.assignments().contains(partition)) {
                return tailer.commit(partition);
            }
        }
        throw new IllegalArgumentException("No tailer matching: " + partition);
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
    public Collection<MQPartition> assignments() {
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

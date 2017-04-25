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
package org.nuxeo.ecm.platform.importer.mqueues.computation.internals.mq;

import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.CQMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

/**
 * @since 9.2
 */
public class StreamMQ implements Stream {

    private final Path basePath;
    private final String name;
    private final int partitions;
    private final MQueues<Record> mQueues;

    public StreamMQ(Path basePath, String name, int partitions) {
        this.basePath = basePath;
        this.name = name;
        this.partitions = partitions;
        this.mQueues = new CQMQueues<>(new File(basePath.toFile(), name), partitions);
    }

    public StreamMQ(Path basePath, String name) {
        this.basePath = basePath;
        this.name = name;
        this.mQueues = new CQMQueues<>(new File(basePath.toFile(), name));
        this.partitions = mQueues.size();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getPartitions() {
        return partitions;
    }

    @Override
    public void appendRecord(String key, byte[] data) {
        Objects.requireNonNull(key);
        long watermark = Watermark.ofTimestamp(System.currentTimeMillis()).getValue();
        mQueues.append((key.hashCode() & 0x7fffffff) % partitions, new Record(key, data, watermark, null));
    }

    @Override
    public void appendRecord(Record record) {
        // yes hash code can be negative
        mQueues.append((record.key.hashCode() & 0x7fffffff) % partitions, record);
    }

    @Override
    public StreamTailer createTailerForPartition(String group, int i) {
        return new StreamTailerMQ(name, mQueues.createTailer(i, group));
    }

    @Override
    public void close() throws Exception {
        if (this.mQueues != null) {
            this.mQueues.close();
        }
    }
}

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
package org.nuxeo.ecm.platform.importer.mqueues.computation.spi;


import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;

/**
 * Stream of {@link Record}.
 * Records are appended to the stream, this can be done concurrently.
 * Consumers ask for a {@link StreamTailer} to read the stream.
 *
 * A stream is composed of a set of partition which is an ordered, immutable sequence of records.
 * Record are distributed into partition according to their {@link Record#key}.
 *
 * Note that Stream is thread safe but not {@link StreamTailer}.
 *
 * @since 9.1
 */
public interface Stream extends AutoCloseable {

    String getName();

    int getPartitions();

    /**
     * Build a {@link Record} with a current timestamp and append it to the stream.
     */
    void appendRecord(String key, byte[] data);

    /**
     * Append a new {@link Record} to the stream.
     */
    void appendRecord(Record record);

    /**
     * Ask a tailer with a group namespace, there should be only one tailer for a group namespace should not
     */
    StreamTailer createTailerForPartition(String group, int partition);


}

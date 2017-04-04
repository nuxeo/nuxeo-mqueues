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
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;

import java.time.Duration;

/**
 * @since 9.1
 */
public class StreamTailerMQ implements StreamTailer {

    private final MQueues.Tailer<Record> tailer;
    private final String streamName;

    public StreamTailerMQ(String streamName, MQueues.Tailer<Record> tailer) {
        this.streamName = streamName;
        this.tailer = tailer;
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public int getPartition() {
        return tailer.getQueue();
    }

    @Override
    public Record read(Duration timeout) throws InterruptedException {
        return tailer.read(timeout);
    }

    @Override
    public void toEnd() {
        tailer.toEnd();
    }

    @Override
    public void toStart() {
        tailer.toStart();
    }

    @Override
    public void toLastCommitted() {
        tailer.toLastCommitted();
    }

    @Override
    public Offset commit() {
        return tailer.commit();
    }

    @Override
    public void close() throws Exception {
        tailer.close();
    }
}

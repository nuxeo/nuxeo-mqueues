package org.nuxeo.ecm.platform.importer.mqueues.computation.internals;/*
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

import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationContext;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @since 9.1
 */
public class ComputationContextImpl implements ComputationContext {
    private final ComputationMetadataMapping metadata;
    private final Map<String, List<Record>> streamRecords;
    private final Map<String, Long> timers;
    private boolean commitFlag = false;
    private long lowWatermark;

    public ComputationContextImpl(ComputationMetadataMapping metadata) {
        this.metadata = metadata;
        this.timers = new HashMap<>();
        this.streamRecords = new HashMap<>();
    }

    public List<Record> getRecords(String streamName) {
        return streamRecords.getOrDefault(streamName, Collections.emptyList());
    }

    public Map<String, Long> getTimers() {
        return timers;
    }

    @Override
    public void setState(String key, byte[] binaryValue) {

    }

    @Override
    public byte[] getState(String key) {
        return new byte[0];
    }

    @Override
    public void setTimer(String key, long time) {
        Objects.requireNonNull(key);
        timers.put(key, time);
    }

    @Override
    public void removeTimer(String key) {
        Objects.requireNonNull(key);
        timers.remove(key);
    }

    @Override
    public void produceRecord(String streamName, String key, byte[] data) {
        produceRecord(streamName, Record.of(key, data));
    }

    @Override
    public void produceRecord(String streamName, Record record) {
        String targetStream = metadata.map(streamName);
        if (!metadata.ostreams.contains(targetStream)) {
            throw new IllegalArgumentException("Stream not registered as output: " + targetStream + ":" + streamName);
        }
        List<Record> records = streamRecords.getOrDefault(targetStream, new ArrayList<>());
        records.add(record);
        streamRecords.putIfAbsent(targetStream, records);
    }

    @Override
    public void setSourceLowWatermark(long watermark) {
        this.lowWatermark = watermark;
    }

    public long getSourceLowWatermark() {
        return lowWatermark;
    }

    public boolean isCommit() {
        return commitFlag;
    }

    @Override
    public void setCommit(boolean commit) {
        commitFlag = commit;
    }

}

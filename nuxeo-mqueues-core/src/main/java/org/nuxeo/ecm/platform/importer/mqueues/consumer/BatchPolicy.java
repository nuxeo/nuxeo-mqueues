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
package org.nuxeo.ecm.platform.importer.mqueues.consumer;

import java.util.concurrent.TimeUnit;

/**
 * Batch policy tells when a batch must be committed.
 *
 * Either it contains the maximum number of messages or the batch is started for too long.
 *
 * @since 9.1
 */
public class BatchPolicy {
    static final BatchPolicy NO_BATCH = new BatchPolicy().capacity(1);
    public static final BatchPolicy DEFAULT = new BatchPolicy().capacity(10).timeThreshold(20, TimeUnit.SECONDS);

    private int capacity;
    private long thresholdMs;

    /**
     * Set the maximum size of the batch.
     */
    public BatchPolicy capacity(int capacity) {
        this.capacity = capacity;
        return this;
    }

    /**
     * Set the time threshold to fill a batch.
     */
    public BatchPolicy timeThreshold(long threshold, TimeUnit unit) {
        this.thresholdMs = TimeUnit.MILLISECONDS.convert(threshold, unit);
        return this;
    }

    public long getThresholdMs() {
        return thresholdMs;
    }

    public int getCapacity() {
        return capacity;
    }
}

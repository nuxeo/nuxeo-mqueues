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

import java.time.Duration;

/**
 * Describe when a batch must be flushed.
 *
 * @since 9.1
 */
public class BatchPolicy {
    static final BatchPolicy NO_BATCH = new Builder(1).build();
    public static final BatchPolicy DEFAULT = new Builder(10).build();

    private final int capacity;
    private final Duration threshold;

    public BatchPolicy(Builder builder) {
        capacity = builder.capacity;
        threshold = builder.threshold;
    }

    public int getCapacity() {
        return capacity;
    }

    public Duration getTimeThreshold() {
        return threshold;
    }

    public static class Builder {
        private static final Duration DEFAULT_THRESHOLD = Duration.ofSeconds(10);
        private final int capacity;
        private Duration threshold = DEFAULT_THRESHOLD;

        /**
         * Set the maximum size of the batch.
         */
        public Builder(int capacity) {
            this.capacity = capacity;
        }

        /**
         * Set the time threshold to fill a batch.
         */
        public Builder timeThreshold(Duration threshold) {
            this.threshold = threshold;
            return this;
        }

        public BatchPolicy build() {
            return new BatchPolicy(this);
        }

    }
}

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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer;

import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.internals.ConsumerRunner;

import java.util.List;

/**
 * The return status of a {@link ConsumerRunner}
 *
 * @since 9.1
 */
public class ConsumerStatus {
    public final int consumer;
    public final long startTime;
    public final long stopTime;
    public final long accepted;
    public final long committed;
    public final long batchFailure;
    public final long batchCommit;
    public final boolean fail;

    public ConsumerStatus(int consumer, long accepted, long committed, long batchCommit, long batchFailure, long startTime, long stopTime, boolean fail) {
        this.consumer = consumer;
        this.accepted = accepted;
        this.committed = committed;
        this.batchCommit = batchCommit;
        this.batchFailure = batchFailure;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.fail = fail;
    }

    @Override
    public String toString() {
        if (fail) {
            return "Consumer status FAILURE";
        }
        double elapsed = (stopTime - startTime) / 1000.;
        double mps = (elapsed != 0) ? committed / elapsed : 0.0;
        return String.format("Consumer %02d status: accepted (include retries): %d, committed: %d, batch: %d, batchFailure: %d, elapsed: %.2fs, throughput: %.2f msg/s.",
                consumer, accepted, committed, batchCommit, batchFailure, elapsed, mps);
    }

    static String toString(List<ConsumerStatus> stats) {
        long startTime = stats.stream().mapToLong(r -> r.startTime).min().orElse(0);
        long stopTime = stats.stream().mapToLong(r -> r.stopTime).min().orElse(0);
        double elapsed = (stopTime - startTime) / 1000.;
        long committed = stats.stream().mapToLong(r -> r.committed).sum();
        double mps = (elapsed != 0) ? committed / elapsed : 0.0;
        int consumers = stats.size();
        long failures = stats.stream().filter(s -> s.fail).count();
        return String.format("Consumers status: threads: %d, failure %d, messages committed: %d, elapsed: %.2fs, throughput: %.2f msg/s",
                consumers, failures, committed, elapsed, mps);

    }
}

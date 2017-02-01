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

import net.jodah.failsafe.RetryPolicy;

/**
 * The consumer policy drive the consumer pool and runner.
 *
 * @since 9.1
 */
public class ConsumerPolicy {
    public enum StartOffset {BEGIN, END, LAST_COMMITTED};
    public static final RetryPolicy NO_RETRY = new RetryPolicy().withMaxRetries(0);

    private BatchPolicy batchPolicy = BatchPolicy.DEFAULT;
    private RetryPolicy retryPolicy = NO_RETRY;
    private boolean skipFailure = false;
    private long waitForMessageMs = 1000;
    private StartOffset startOffset = StartOffset.LAST_COMMITTED;

    public ConsumerPolicy() {
    }

    public BatchPolicy getBatchPolicy() {
        return batchPolicy;
    }

    public ConsumerPolicy batchPolicy(BatchPolicy batchPolicy) {
        this.batchPolicy = batchPolicy;
        return this;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public ConsumerPolicy retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public boolean continueOnFailure() {
        return skipFailure;
    }

    public ConsumerPolicy continueOnFailure(boolean continueOnFailure) {
        this.skipFailure = continueOnFailure;
        return this;
    }

    public long getWaitForMessageMs() {
        return waitForMessageMs;
    }

    public ConsumerPolicy waitForMessageMs(long waitForMessageMs) {
        this.waitForMessageMs = waitForMessageMs;
        return this;
    }

    public StartOffset getStartOffset() {
        return startOffset;
    }

    public ConsumerPolicy startOffset(StartOffset startOffset) {
        this.startOffset = startOffset;
        return this;
    }
}

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

import java.time.Duration;

/**
 * The consumer policy drive the consumer pool and runner.
 *
 * @since 9.1
 */
public class ConsumerPolicy {
    public enum StartOffset {BEGIN, END, LAST_COMMITTED}

    public static final RetryPolicy NO_RETRY = new RetryPolicy().withMaxRetries(0);
    public static final ConsumerPolicy DEFAULT = new Builder().build();

    private final BatchPolicy batchPolicy;
    private final RetryPolicy retryPolicy;
    private final boolean skipFailure;
    private final Duration waitMessageTimeout;
    private final StartOffset startOffset;

    public ConsumerPolicy(Builder builder) {
        batchPolicy = builder.batchPolicy;
        retryPolicy = builder.retryPolicy;
        skipFailure = builder.skipFailure;
        waitMessageTimeout = builder.waitMessageTimeout;
        startOffset = builder.startOffset;
    }

    public BatchPolicy getBatchPolicy() {
        return batchPolicy;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public boolean continueOnFailure() {
        return skipFailure;
    }

    public Duration getWaitMessageTimeout() {
        return waitMessageTimeout;
    }

    public StartOffset getStartOffset() {
        return startOffset;
    }

    public static class Builder {
        private BatchPolicy batchPolicy = BatchPolicy.DEFAULT;
        private RetryPolicy retryPolicy = NO_RETRY;
        private boolean skipFailure = false;
        private Duration waitMessageTimeout = Duration.ofSeconds(2);
        private StartOffset startOffset = StartOffset.LAST_COMMITTED;

        public Builder() {

        }

        public Builder batchPolicy(BatchPolicy policy) {
            batchPolicy = policy;
            return this;
        }

        public Builder retryPolicy(RetryPolicy policy) {
            retryPolicy = policy;
            return this;
        }

        /**
         * Continue on next message even if the retry policy has failed.
         */
        public Builder continueOnFailure(boolean value) {
            skipFailure = value;
            return this;
        }

        /**
         * Consumer will stop if there is no more message after this timeout.
         */
        public Builder waitMessageTimeout(Duration duration) {
            waitMessageTimeout = duration;
            return this;
        }

        /**
         * Consumer will wait for ever message.
         */
        public Builder waitMessageForEver() {
            waitMessageTimeout = Duration.ofSeconds(Integer.MAX_VALUE);
            return this;
        }

        /**
         * Where to read the first message.
         */
        public Builder startOffset(StartOffset startOffset) {
            this.startOffset = startOffset;
            return this;
        }

        public ConsumerPolicy build() {
            return new ConsumerPolicy(this);
        }
    }
}

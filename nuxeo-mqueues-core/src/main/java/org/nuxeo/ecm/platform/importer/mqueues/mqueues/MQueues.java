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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues;

import org.nuxeo.ecm.platform.importer.mqueues.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * A MQueues (for Multiple Queues) is a set of unbounded persisted queues.
 *
 * This enable one or multiple producers to dipatch {@link Message} on different queues.
 *
 * Using a {@link Tailer} a consumer can read message, the current position of a tailer can be committed.
 *
 * A common pattern is to choose the queue size equals to the number of concurrent consumers.
 *
 * @since 9.1
 */
public interface MQueues<M extends Message> extends AutoCloseable {

    /**
     * Sequential reader from a queue.
     *
     * A tailer is not thread safe and must be used only by one consumer.
     *
     */
    interface Tailer<M> {

        /**
         * Get a message from the queue within the timeout.
         *
         * @return null if there is no message in the queue after the timeout.
         */
        M get(long timeout, TimeUnit unit) throws InterruptedException;

        /**
         * Commit the offset of the last message returned by get.
         */
        void commit();

        /**
         * Go to the end of the queue.
         */
        void toEnd();

        /**
         * Go to the beginning of the queue.
         */
        void toStart();

        /**
         * Go just after the last committed message.
         */
        void toLastCommitted();

        /**
         * Returns the associated queue index.
         */
        int getQueue();
    }

    /**
     * Returns the number of queues.
     *
     */
    int size();

    /**
     * Put a message into a queue.
     *
     * This is thread safe, a queue can be shared by multiple producers.
     *
     * @param queue index starting from 0 and lower than {@link #size()}
     */
    void put(int queue, M message);

    /**
     * Get a {@link Tailer} associed to a queue index.
     *
     * This call is thread safe but {@link Tailer} are not.
     */
    Tailer<M> getTailer(int queue);

}

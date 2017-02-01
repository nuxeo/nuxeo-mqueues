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
 * Producers can dispatch {@link Message} on different queues.
 *
 * Consumer read {@link Message} using a {@link Tailer}, the position of the tailer can be persisted.
 *
 * @since 9.1
 */
public interface MQueues<M extends Message> extends AutoCloseable {

    /**
     * Returns the size of the mqueues: the number of queues.
     *
     */
    int size();

    /**
     * Append a message into a queue, returns an {@link Offset}.
     *
     * This method is thread safe, a queue can be shared by multiple producers.
     *
     * @param queue index lower than {@link #size()}
     */
    Offset append(int queue, M message);

    /**
     * Create a new {@link Tailer} associed with the queue index.
     *
     * The committed offset is presisted in the default namespace.
     *
     * There can be one and only one consumer for queue in a namespace.
     *
     * A tailer is not thread safe.
     *
     */
    Tailer<M> createTailer(int queue);

    /**
     * Create a new {@link Tailer} associed to a queue index, using a specified offset name space.
     *
     * The committed offset position is shared by all tailers of the same queue with the same name.
     *
     * There can be one and only one consumer for queue in a namespace
     *
     * A tailer is not thread safe.
     *
     */
    Tailer<M> createTailer(int queue, String name);

    /**
     * Wait for consumer to process a message up to the offset.
     *
     * The message is processed if a consumer commit its offset (or a bigger one) in the default name space.
     *
     * Return true if the message has been consumed, false in case of timeout.
     */
    boolean waitFor(Offset offset, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Sequential reader for a queue.
     *
     * A tailer is not thread safe and should not be shared by multiple threads.
     *
     */
    interface Tailer<M> extends AutoCloseable {

        /**
         * Read a message from the queue within the timeout.
         *
         * @return null if there is no message in the queue after the timeout.
         */
        M read(long timeout, TimeUnit unit) throws InterruptedException;

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
         * Commit the offset of the last message returned by read.
         */
        Offset commit();

        /**
         * Returns the associated queue index.
         */
        int getQueue();
    }

}

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

import java.io.Externalizable;
import java.time.Duration;
import java.util.Objects;

/**
 *
 * @since 9.2
 */
public interface MQAppender<M extends Externalizable> extends AutoCloseable {

    /**
     * Returns the MQueue name.
     *
     */
    String getName();

    /**
     * Returns the size of the queues array (aka number of partitions).
     *
     */
    int size();

    /**
     * Append a message into a queue, returns current {@link MQOffset} position.
     *
     * This method is thread safe, a queue can be shared by multiple producers.
     *
     * @param queue index lower than {@link #size()}
     */
    MQOffset append(int queue, M message);

    /**
     * Same as {@link #append(int, Externalizable)}, the queue is chosen using a hash of {@param key}.
     */
    default MQOffset append(String key, M message) {
        Objects.requireNonNull(key);
        // Provide a basic partitioning that works because:
        // 1. String.hashCode is known to be constant even with different JVM (this is not the case for all objects)
        // 2. the mod is not optimal in case of partition rebalancing but the size is not supposed to change
        // and yes hashCode can be negative.
        int queue = (key.hashCode() & 0x7fffffff) % size();
        return append(queue, message);
    }

    /**
     * Wait for consumer to process a message up to the offset.
     *
     * The message is processed if a consumer commit its offset (or a bigger one) in the default name space.
     *
     * Return true if the message has been consumed, false in case of timeout.
     */
    boolean waitFor(MQOffset offset, String nameSpace, Duration timeout) throws InterruptedException;

    boolean closed();
}

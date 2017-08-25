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
package org.nuxeo.lib.core.mqueues.mqueues;

import java.io.Externalizable;
import java.time.Duration;
import java.util.Collection;

/**
 * Sequential reader for a partition or multiple partitions.
 *
 * A tailer is not thread safe and should not be shared by multiple threads.
 *
 */
public interface MQTailer<M extends Externalizable> extends AutoCloseable {

    /**
     * Returns the consumer group.
     *
     */
    String group();

    /**
     * Returns the list of MQueue name/partition tuples currently assigned to this tailer.
     * Assignments can change only if the tailer has been created using {@link MQManager#subscribe}.
     */
    Collection<MQPartition> assignments();

    /**
     * Read a message from assigned partitions within the timeout.
     *
     * @return null if there is no message in the queue after the timeout.
     * @throws MQRebalanceException if a partition rebalancing happen during the read,
     * this is possible only when using {@link MQManager#subscribe}.
     */
    MQRecord<M> read(Duration timeout) throws InterruptedException;

    /**
     * Commit current positions for all partitions (last message offset returned by read).
     */
    void commit();

    /**
     * Commit current position for the partition.
     *
     * @return the committed offset, can return null if there was no previous read done on this partition.
     */
    MQOffset commit(MQPartition partition);

    /**
     * Set the current positions to the end of all partitions.
     */
    void toEnd();

    /**
     * Set the current position to the fist message of all partitions.
     */
    void toStart();

    /**
     * Set the current positions to previously committed positions.
     */
    void toLastCommitted();

    /**
     * Returns {@code true} if the tailer has been closed.
     */
    boolean closed();
}

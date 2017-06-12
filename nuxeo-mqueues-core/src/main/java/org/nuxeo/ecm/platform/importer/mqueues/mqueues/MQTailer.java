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
import java.util.Collection;

/**
 * Sequential reader for a mqueue/partition or a list of mqueue/partition.
 *
 * A tailer is not thread safe and should not be shared by multiple threads.
 *
 */
public interface MQTailer<M extends Externalizable> extends AutoCloseable {

    /**
     * Read a message from the assigned mqueue/partition within the timeout.
     *
     * @return null if there is no message in the queue after the timeout.
     */
    MQRecord<M> read(Duration timeout) throws InterruptedException;

    /**
     * Commit the offset of the last message returned by read for this mqueue/partition.
     */
    MQOffset commit(MQPartition partition);

    /**
     * Commit the offset of the last message returned by read for all the assigned mqueue/partition.
     */
    void commit();

    /**
     * Position the current offsets of all mqueue/partition to the end.
     */
    void toEnd();

    /**
     * Position the current offsets of all mqueue/partition to the beginning.
     */
    void toStart();

    /**
     * Position the current offsets of all mqueue/partition just after the last committed message.
     */
    void toLastCommitted();

    /**
     * Returns the list of mqueue/partition currently assigned to this tailer.
     *
     */
    Collection<MQPartition> assignments();

    /**
     * Returns the tailer group.
     *
     */
    String getGroup();

    /**
     * Returns true if the tailer has been closed using {@link #close()}
     */
    boolean closed();

}

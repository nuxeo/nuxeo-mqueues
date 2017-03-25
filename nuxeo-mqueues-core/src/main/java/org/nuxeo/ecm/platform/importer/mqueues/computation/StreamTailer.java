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
package org.nuxeo.ecm.platform.importer.mqueues.computation;

import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;

import java.time.Duration;

/**
 * A tailer for a partition of a Stream.
 * This is not thread safe.
 *
 * @since 9.1
 */
public interface StreamTailer extends AutoCloseable {

    /**
     * Returns the associated stream.
     */
    String getStreamName();

    /**
     * Returns the associated partition stream.
     */
    int getPartition();


    /**
     * Read a message from the queue within the timeout.
     *
     * @return null if there is no message in the queue after the timeout.
     */
    Record read(Duration timeout) throws InterruptedException;

    /**
     * Go to the end of the Stream partition.
     */
    void toEnd();

    /**
     * Go to the beginning of the Stream partition.
     */
    void toStart();

    /**
     * Go just after the last committed record.
     */
    void toLastCommitted();

    /**
     * Commit the offset of the last record returned by read.
     */
    Offset commit();

}


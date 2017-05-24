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

import java.time.Duration;

/**
 * Sequential reader for a queue.
 *
 * A tailer is not thread safe and should not be shared by multiple threads.
 *
 */
public interface MQTailer<M> extends AutoCloseable {

    /**
     * Read a message from the queue within the timeout.
     *
     * @return null if there is no message in the queue after the timeout.
     */
    M read(Duration timeout) throws InterruptedException;

    /**
     * Commit the offset of the last message returned by read.
     */
    MQOffset commit();

    /**
     * Position the current offset to the end of queue.
     */
    void toEnd();

    /**
     * Position the current offset to the beginning of the queue.
     */
    void toStart();

    /**
     * Position the current offset just after the last committed message.
     */
    void toLastCommitted();


    /**
     * Returns the associated queue index.
     */
    int getQueue();

    /**
     * Returns the name of the MQueue.
     *
     */
    String getMQueueName();

    /**
     * Return the tailer name space.
     *
     */
    String getNameSpace();
}

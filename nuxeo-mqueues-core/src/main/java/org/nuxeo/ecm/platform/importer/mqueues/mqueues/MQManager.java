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

/**
 * Manage appender and tailer access.
 *
 * @since 9.2
 */
public interface MQManager<M extends Externalizable> extends AutoCloseable {

    /**
     * Check if a MQueue exists.
     */
    boolean exists(String name);

    /**
     * Create a new MQueue if it does not exists.
     */
    boolean createIfNotExists(String name, int size);

    /**
     * Try to delete a MQueue.
     * Returns true if successfully deleted, might not be possible depending on the implementation.
     */
    boolean delete(String name);

    /**
     * Get an appender on the MQueue, The appender is thread safe.
     */
    MQAppender<M> getAppender(String name);

    /**
     * Create a tailer on a mqueue/partition using a group
     */
    MQTailer<M> createTailer(String group, MQPartition partition);


}

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
import java.util.Collection;

/**
 * Manage MQueue and give access to appender and tailers.
 *
 * @since 9.2
 */
public interface MQManager<M extends Externalizable> extends AutoCloseable {

    /**
     * Returns {@code true} if a MQueue with this {@code name} exists.
     */
    boolean exists(String name);

    /**
     * Creates a new MQueue with {@code size} partitions if the MQueue does not exists.
     * Returns true it the MQueue has been created.
     */
    boolean createIfNotExists(String name, int size);

    /**
     * Try to delete a MQueue.
     * Returns true if successfully deleted, might not be possible depending on the implementation.
     */
    boolean delete(String name);

    /**
     * Get an appender for the MQueue named {@code name}.
     * An appender is thread safe.
     */
    MQAppender<M> getAppender(String name);

    /**
     * Create a tailer for a consumer {@code group} and assign a single {@code partition}.
     * A tailer is NOT thread safe.
     */
    MQTailer<M> createTailer(String group, MQPartition partition);

    /**
     * Create a tailer for a consumer {@code group} and assign multiple {@code partitions}.
     * A tailer is NOT thread safe.
     */
    MQTailer<M> createTailer(String group, Collection<MQPartition> partitions);

    /**
     * Returns {@code true} if the MQueue {@link #subscribe} method is supported.
     */
    boolean supportSubscribe();

    /**
     * Create a tailer for a consumer {@code group} and subscribe to multiple MQueues.
     * The partitions assignment is done dynamically depending on the number of subscribers.
     * The partitions can change during tailers life, this is called a rebalancing.
     * A listener can be used to be notified on assignment changes.
     * <p/>
     * A tailer is NOT thread safe.
     * <p/>
     * You should not mix {@link #createTailer} and {@code subscribe} usage using the same {@code group}.
     */
    MQTailer<M> subscribe(String group, Collection<String> names, MQRebalanceListener listener);

}

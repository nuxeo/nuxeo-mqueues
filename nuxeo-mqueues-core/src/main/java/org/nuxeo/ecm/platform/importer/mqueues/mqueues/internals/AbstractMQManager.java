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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals;


import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRebalanceListener;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;

import java.io.Externalizable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public abstract class AbstractMQManager<M extends Externalizable> implements MQManager<M> {
    private final Map<String, MQAppender<M>> appenders = new ConcurrentHashMap<>();
    private final Map<MQPartitionGroup, MQTailer<M>> tailersAssignments = new ConcurrentHashMap<>();
    private final Set<MQTailer<M>> tailers = Collections.newSetFromMap(new ConcurrentHashMap<MQTailer<M>, Boolean>());

    protected abstract void create(String name, int size);

    protected abstract MQAppender<M> createAppender(String name);

    protected abstract MQTailer<M> acquireTailer(Collection<MQPartition> partitions, String group);

    protected abstract MQTailer<M> doSubscribe(String group, Collection<String> names, MQRebalanceListener listener);

    @Override
    public boolean createIfNotExists(String name, int size) {
        if (!exists(name)) {
            create(name, size);
            return true;
        }
        return false;
    }

    @Override
    public boolean delete(String name) {
        return false;
    }

    @Override
    public MQTailer<M> createTailer(String group, Collection<MQPartition> partitions) {
        partitions.forEach(partition -> checkTailerForPartition(group, partition));
        MQTailer<M> ret = acquireTailer(partitions, group);
        partitions.forEach(partition -> tailersAssignments.put(new MQPartitionGroup(group, partition), ret));
        tailers.add(ret);
        return ret;
    }

    @Override
    public boolean supportSubscribe() {
        return false;
    }

    @Override
    public MQTailer<M> subscribe(String group, Collection<String> names, MQRebalanceListener listener) {
        MQTailer<M> ret = doSubscribe(group, names, listener);
        tailers.add(ret);
        return ret;
    }


    private void checkTailerForPartition(String group, MQPartition partition) {
        MQPartitionGroup key = new MQPartitionGroup(group, partition);
        MQTailer<M> ret = tailersAssignments.get(key);
        if (ret != null && !ret.closed()) {
            throw new IllegalArgumentException("Tailer for this partition already created: " + partition + ", group: " + group);
        }
        if (!exists(partition.name())) {
            throw new IllegalArgumentException("Tailer with unknown MQueue name: " + partition.name());
        }
    }

    @Override
    public MQTailer<M> createTailer(String group, MQPartition partition) {
        return createTailer(group, Collections.singletonList(partition));
    }

    @Override
    public synchronized MQAppender<M> getAppender(String name) {
        if (!appenders.containsKey(name) || appenders.get(name).closed()) {
            if (exists(name)) {
                appenders.put(name, createAppender(name));
            } else {
                throw new IllegalArgumentException("unknown MQueue name: " + name);
            }
        }
        return appenders.get(name);
    }


    @Override
    public void close() throws Exception {
        for (MQAppender<M> app : appenders.values()) {
            app.close();
        }
        appenders.clear();
        for (MQTailer<M> tailer : tailers) {
            tailer.close();
        }
        tailers.clear();
        tailersAssignments.clear();
    }
}

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage {@link MQueue} access.
 *
 * @since 9.2
 */
public abstract class MQManager<M extends Externalizable> implements AutoCloseable {
    private final Map<String, MQueue<M>> mqueues = new ConcurrentHashMap<>();

    /**
     * Check if a MQueue exists.
     */
    public abstract boolean exists(String name);

    /**
     * Open an existing MQueue.
     */
    public abstract MQueue<M> open(String name);

    /**
     * Create a new MQueue.
     */
    public abstract MQueue<M> create(String name, int size);

    /**
     * Open an existing MQueue or create it.
     */
    public synchronized MQueue<M> openOrCreate(String name, int size) {
        if (!mqueues.containsKey(name)) {
            if (exists(name)) {
                mqueues.put(name, open(name));
            } else {
                mqueues.put(name, create(name, size));
            }
        }
        return get(name);
    }

    /**
     * Getter for a MQueue created or opened.
     */
    public MQueue<M> get(String name) {
        return mqueues.get(name);
    }


    @Override
    public void close() throws Exception {
        // TODO: check if we want this behavior, closing the manager close all MQueue
        for (MQueue<M> mq : mqueues.values()) {
            mq.close();
        }
        mqueues.clear();
    }
}

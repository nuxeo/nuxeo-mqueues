package org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle;/*
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

import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQOffsetImpl;

import java.io.Externalizable;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @since 9.1
 */
public class ChronicleMQTailer<M extends Externalizable> implements MQTailer<M> {
    private static final Log log = LogFactory.getLog(ChronicleMQTailer.class);
    private static final long POLL_INTERVAL_MS = 100L;
    private final String basePath;
    private final ExcerptTailer tailer;
    private final String nameSpace;
    private final int queueIndex;
    private final ChronicleMQOffsetTracker offsetTracker;
    private boolean closed = false;

    // keep track of all tailers on the same namespace index even from different mq
    private static final Set<String> indexNamespace = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public ChronicleMQTailer(String basePath, ExcerptTailer tailer, int queue, String nameSpace) {
        Objects.requireNonNull(nameSpace);
        this.basePath = basePath;
        this.tailer = tailer;
        this.queueIndex = queue;
        this.nameSpace = nameSpace;
        registerTailer();
        this.offsetTracker = new ChronicleMQOffsetTracker(basePath, queue, this.nameSpace);
        toLastCommitted();
    }

    private void registerTailer() {
        String key = getTailerKey();
        if (!indexNamespace.add(key)) {
            throw new IllegalArgumentException("A tailer for this queue and namespace already exists: " + key);
        }
    }

    private void unregisterTailer() {
        String key = getTailerKey();
        indexNamespace.remove(key);
    }

    private String getTailerKey() {
        return basePath + " " + queueIndex + " " + nameSpace;
    }

    @Override
    public M read(Duration timeout) throws InterruptedException {
        M ret = read();
        if (ret != null) {
            return ret;
        }
        final long timeoutMs = timeout.toMillis();
        final long deadline = System.currentTimeMillis() + timeoutMs;
        final long delay = Math.min(POLL_INTERVAL_MS, timeoutMs);
        while (ret == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(delay);
            ret = read();
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    private M read() {
        if (closed) {
            throw new IllegalStateException("The tailer has been closed.");
        }
        final List<M> ret = new ArrayList<>(1);
        if (tailer.readDocument(w -> ret.add((M) w.read("msg").object()))) {
            return ret.get(0);
        }
        return null;
    }

    @Override
    public MQOffset commit() {
        // we write raw: queue, offset, timestamp
        long offset = tailer.index();
        offsetTracker.commit(offset);
        if (log.isTraceEnabled()) {
            log.trace(String.format("queue-%02d commit offset: %d", queueIndex, offset));
        }
        return new MQOffsetImpl(queueIndex, offset);
    }

    @Override
    public void toEnd() {
        log.debug(String.format("queue-%02d toEnd", queueIndex));
        tailer.toEnd();
    }

    @Override
    public void toStart() {
        log.debug(String.format("queue-%02d toStart", queueIndex));
        tailer.toStart();
    }

    @Override
    public void toLastCommitted() {
        long offset = offsetTracker.getLastCommittedOffset();
        if (offset > 0) {
            log.debug(String.format("queue-%02d toLastCommitted found: %d", queueIndex, offset));
            tailer.moveToIndex(offset);
        } else {
            log.debug(String.format("queue-%02d toLastCommitted not found, run from beginning", queueIndex));
            tailer.toStart();
        }
    }

    @Override
    public int getQueue() {
        return queueIndex;
    }

    @Override
    public String getMQueueName() {
        // TODO get it from mqueues
        return new File(basePath).getName();
    }

    @Override
    public String getNameSpace() {
        return nameSpace;
    }

    @Override
    public void close() throws Exception {
        offsetTracker.close();
        unregisterTailer();
        closed = true;
    }

    @Override
    public boolean closed() {
        return closed;
    }
}

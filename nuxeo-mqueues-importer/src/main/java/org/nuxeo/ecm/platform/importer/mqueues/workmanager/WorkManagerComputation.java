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
package org.nuxeo.ecm.platform.importer.mqueues.workmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.work.WorkManagerImpl;
import org.nuxeo.ecm.core.work.WorkQueueRegistry;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.core.work.api.WorkQueueMetrics;
import org.nuxeo.ecm.core.work.api.WorkSchedulePath;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Settings;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.ComputationManagerImpl;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.mq.StreamsMQ;
import org.nuxeo.runtime.api.Framework;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * @since 9.2
 */
public class WorkManagerComputation extends WorkManagerImpl {
    protected static final Log log = LogFactory.getLog(WorkManagerComputation.class);
    protected static final int DEFAULT_CONCURRENCY = 4;
    protected Topology topology;
    protected Settings settings;
    protected ComputationManagerImpl manager;
    protected StreamsMQ streams;
    protected WorkQueueRegistry wqRegistry;

    @Override
    public void schedule(Work work, Scheduling scheduling, boolean afterCommit) {
        if (log.isDebugEnabled()) {
            log.debug("SCHEDULE " + work + ", to: " + work.getCategory() + ", scheduling: " + scheduling + " after commait: " + afterCommit);
        }
        String queueId = getCategoryQueueId(work.getCategory());
        if (!isQueuingEnabled(queueId)) {
            return;
        }
        if (afterCommit && scheduleAfterCommit(work, scheduling)) {
            return;
        }
        WorkSchedulePath.newInstance(work);
        // TODO: take in account the scheduling state when possible

        // TODO choose a key with a transaction id so all jobs from the same tx are ordered ?
        String key = work.getId();
        streams.getStream(work.getCategory()).
                appendRecord(key, WorkComputation.serialize(work));
    }

    @Override
    public void init() {
        if (started) {
            return;
        }
        log.debug("init");
        synchronized (this) {
            if (started) {
                return;
            }
            this.wqRegistry = loadRegistry();
            initTopology();
            initStream();
            startComputation();
            started = true;
            log.info("Initialized");
        }
    }

    /**
     * Hack to get work queue contributions from the pristine WorkManagerImpl
     */
    protected WorkQueueRegistry loadRegistry() {
        WorkManagerImpl wmi = (WorkManagerImpl) Framework.getRuntime().getComponent("org.nuxeo.ecm.core.work.service");
        Class clazz = WorkManagerImpl.class;
        Field protectedField;
        try {
            protectedField = clazz.getDeclaredField("workQueueConfig");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        protectedField.setAccessible(true);
        WorkQueueRegistry ret;
        try {
            ret = (WorkQueueRegistry) protectedField.get(wmi);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public void initStream() {
        File dir = new File(Framework.getRuntime().getHome(), "data/streams");
        log.info("Init WorkManager Streams in: " + dir.getAbsolutePath());
        streams = new StreamsMQ(dir.toPath());
    }

    protected void startComputation() {
        this.manager = new ComputationManagerImpl(streams, topology, settings);
        manager.start();
    }

    protected void initTopology() {
        Topology.Builder builder = Topology.builder();
        wqRegistry.getQueueIds().forEach(item -> builder.addComputation(() -> new WorkComputation(item), Collections.singletonList("i1:" + item)));
        this.topology = builder.build();
        this.settings = new Settings(DEFAULT_CONCURRENCY);
        wqRegistry.getQueueIds().forEach(item -> settings.setConcurrency(item, wqRegistry.get(item).getMaxThreads()));
    }

    @Override
    public boolean shutdownQueue(String queueId, long timeout, TimeUnit unit) throws InterruptedException {
        log.info("Shutdown WorkManager stream: " + queueId);
        // TODO: decide what to do ?
        return false;
    }

    @Override
    public boolean shutdown(long timeout, TimeUnit timeUnit) throws InterruptedException {
        log.info("Shutdown WorkManager in " + timeUnit.toMillis(timeout) + " ms");
        boolean ret = manager.stop(Duration.ofMillis(timeUnit.toMillis(timeout)));
        try {
            streams.close();
        } catch (Exception e) {
            log.error("Error while closing workmanager streams", e);
        }
        return ret;
    }

    @Override
    public int getQueueSize(String s, Work.State state) {
        return 0;
    }

    @Override
    public WorkQueueMetrics getMetrics(String s) {
        return null;
    }

    @Override
    public boolean awaitCompletion(String s, long l, TimeUnit timeUnit) throws InterruptedException {
        return true;
    }

    @Override
    public boolean awaitCompletion(long l, TimeUnit timeUnit) throws InterruptedException {
        return true;
    }

    @Override
    public Work.State getWorkState(String s) {
        return null;
    }

    @Override
    public Work find(String s, Work.State state) {
        return null;
    }

    @Override
    public List<Work> listWork(String s, Work.State state) {
        return null;
    }

    @Override
    public List<String> listWorkIds(String s, Work.State state) {
        return null;
    }

}
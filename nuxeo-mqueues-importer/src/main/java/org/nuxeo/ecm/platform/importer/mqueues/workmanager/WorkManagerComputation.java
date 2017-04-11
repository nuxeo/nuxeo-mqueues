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
import org.nuxeo.ecm.core.work.api.WorkQueueDescriptor;
import org.nuxeo.ecm.core.work.api.WorkQueueMetrics;
import org.nuxeo.ecm.core.work.api.WorkSchedulePath;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Settings;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.ComputationManagerImpl;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.mq.StreamsMQ;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Stream;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.naming.NamingException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;


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

    public class WorkScheduling implements Synchronization {
        public final Work work;
        public final Scheduling scheduling;


        public WorkScheduling(Work work, Scheduling scheduling) {
            this.work = work;
            this.scheduling = scheduling;
        }

        public void beforeCompletion() {
        }

        public void afterCompletion(int status) {
            if (status == 3) {
                WorkManagerComputation.this.schedule(this.work, this.scheduling, false);
            } else {
                if (status != 4) {
                    throw new IllegalArgumentException("Unsupported transaction status " + status);
                }
            }

        }
    }

    @Override
    public void schedule(Work work, Scheduling scheduling, boolean afterCommit) {
        if (log.isDebugEnabled()) {
            log.debug("SCHEDULE " + work + ", to: " + work.getCategory() + ", scheduling: " + scheduling + " after commait: " + afterCommit);
        }
        String queueId = getCategoryQueueId(work.getCategory());
        // TODO: YYYY test why this check need to be removed
        if (!isQueuingEnabled(queueId)) {
            return;
        }
        if (afterCommit && scheduleAfterCommit(work, scheduling)) {
            System.out.println("OUT");
            return;
        }
        WorkSchedulePath.newInstance(work);
        // TODO: take in account the scheduling state when possible

        // TODO choose a key with a transaction id so all jobs from the same tx are ordered ?
        String key = work.getId();
        Stream stream = streams.getStream(work.getCategory());
        if (stream == null) {
            log.error("Unknown stream (work category): " + work.getCategory());
            return;
        }
        stream.appendRecord(key, ComputationWork.serialize(work));
    }

    @Override
    public boolean isQueuingEnabled(String queueId) {
        WorkQueueDescriptor wqd = this.getWorkQueueDescriptor(queueId);
        return wqd != null && wqd.isQueuingEnabled();
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

    protected void initStream() {
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
        wqRegistry.getQueueIds().forEach(item -> builder.addComputation(() -> new ComputationWork(item), Collections.singletonList("i1:" + item)));
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


    protected boolean scheduleAfterCommit(Work work, Scheduling scheduling) {
        TransactionManager transactionManager;
        try {
            transactionManager = TransactionHelper.lookupTransactionManager();
        } catch (NamingException var7) {
            transactionManager = null;
        }

        if (transactionManager == null) {
            if (log.isDebugEnabled()) {
                log.debug("Not scheduling work after commit because of missing transaction manager: " + work);
            }

            return false;
        } else {
            try {
                Transaction transaction = transactionManager.getTransaction();
                if (transaction == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Not scheduling work after commit because of missing transaction: " + work);
                    }

                    return false;
                } else {
                    int status = transaction.getStatus();
                    if (status == 0) {
                        if (log.isDebugEnabled()) {
                            log.debug("Scheduling work after commit: " + work);
                        }
                        transaction.registerSynchronization(new WorkManagerImpl.WorkScheduling(work, scheduling));
                        return true;
                    } else if (status == 3) {
                        if (log.isDebugEnabled()) {
                            log.debug("Scheduling work immediately: " + work);
                        }

                        return false;
                    } else if (status == 1) {
                        if (log.isDebugEnabled()) {
                            log.debug("Cancelling schedule because transaction marked rollback-only: " + work);
                        }

                        return true;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Not scheduling work after commit because transaction is in status " + status + ": " + work);
                        }

                        return false;
                    }
                }
            } catch (RollbackException | SystemException var6) {
                log.error("Cannot schedule after commit", var6);
                return false;
            }
        }
    }

}
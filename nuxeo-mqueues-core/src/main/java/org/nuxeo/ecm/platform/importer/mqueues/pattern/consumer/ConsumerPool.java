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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.Message;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.internals.AbstractCallablePool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.internals.ConsumerRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Run a pool of ConsumerRunner.
 *
 * @since 9.1
 */
public class ConsumerPool<M extends Message> extends AbstractCallablePool<ConsumerStatus> {
    private static final Log log = LogFactory.getLog(ConsumerPool.class);
    private final MQManager<M> manager;
    private final ConsumerFactory<M> factory;
    private final ConsumerPolicy policy;
    private final String mqName;
    private final List<List<MQPartition>> defaultAssignements;

    public ConsumerPool(String mqName, MQManager<M> manager, ConsumerFactory<M> factory, ConsumerPolicy policy) {
        super(computeNbThreads((short) manager.getAppender(mqName).size(), policy.getMaxThreads()));
        this.mqName = mqName;
        this.manager = manager;
        this.factory = factory;
        this.policy = policy;
        this.defaultAssignements = getDefaultAssignments();
        if (manager.supportSubscribe()) {
            log.info("Creating consumer pool using MQ subscribe on " + mqName);
        } else {
            log.info("Creating consumer pool using MQ assignments on " + mqName + ": " + defaultAssignements);
        }
    }

    protected static short computeNbThreads(short maxConcurrency, short maxThreads) {
        if (maxThreads > 0) {
            return (short) Math.min(maxConcurrency, maxThreads);
        }
        return maxConcurrency;
    }

    public String getConsumerGroupName() {
        return policy.getName();
    }

    @Override
    protected ConsumerStatus getErrorStatus() {
        return new ConsumerStatus("error", 0, 0, 0, 0, 0, 0, true);
    }

    @Override
    protected Callable<ConsumerStatus> getCallable(int i) {
        return new ConsumerRunner<>(factory, policy, manager, defaultAssignements.get(i));
    }

    @Override
    protected String getThreadPrefix() {
        return "Nuxeo-Consumer";
    }

    @Override
    protected void afterCall(List<ConsumerStatus> ret) {
        ret.forEach(log::warn);
        log.warn(ConsumerStatus.toString(ret));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private List<List<MQPartition>> getDefaultAssignments() {
        Map<String, Integer> streams = Collections.singletonMap(mqName, manager.getAppender(mqName).size());
        return KafkaUtils.roundRobinAssignments(getNbThreads(), streams);
    }

}

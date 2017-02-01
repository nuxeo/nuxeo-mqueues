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
package org.nuxeo.ecm.platform.importer.mqueues.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.AbstractCallablePool;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * A Pool of ProducerRunner
 *
 * @since 9.1
 */
public class ProducerPool<M extends Message> extends AbstractCallablePool<ProducerStatus> {
    private static final Log log = LogFactory.getLog(ProducerPool.class);
    private final MQueues<M> mq;
    private final ProducerFactory<M> factory;

    public ProducerPool(MQueues<M> mq, ProducerFactory<M> factory, int nbThreads) {
        super(nbThreads);
        this.mq = mq;
        this.factory = factory;
    }

    @Override
    protected ProducerStatus getErrorStatus() {
        return new ProducerStatus(0, 0, 0, 0, true);
    }

    @Override
    protected Callable<ProducerStatus> getCallable(int i) {
        return new ProducerRunner<>(factory, mq, i);
    }

    @Override
    protected String getThreadPrefix() {
        return "Nuxeo-Producer";
    }

    @Override
    protected void afterCall(List<ProducerStatus> ret) {
        ret.forEach(log::info);
        log.warn(ProducerStatus.toString(ret));
    }


}

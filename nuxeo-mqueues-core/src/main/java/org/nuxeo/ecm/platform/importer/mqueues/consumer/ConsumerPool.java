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
package org.nuxeo.ecm.platform.importer.mqueues.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Run a pool of ConsumerRunner.
 *
 * @since 9.1
 */
public class ConsumerPool<M extends Message> extends AbstractCallablePool<ConsumerStatus> {
    private static final Log log = LogFactory.getLog(ConsumerPool.class);
    private final MQueues<M> qm;
    private final ConsumerFactory<M> factory;
    private final ConsumerPolicy policy;
    private final List<MQueues.Tailer<M>> tailers;

    public ConsumerPool(MQueues<M> qm, ConsumerFactory<M> factory, ConsumerPolicy policy) {
        super(qm.size());
        this.qm = qm;
        this.factory = factory;
        this.policy = policy;
        this.tailers = new ArrayList<>(qm.size());
    }

    @Override
    protected ConsumerStatus getErrorStatus() {
        return new ConsumerStatus(0, 0, 0, 0, 0, 0, 0, true);
    }

    @Override
    protected Callable<ConsumerStatus> getCallable(int i) {
        MQueues.Tailer<M> tailer = qm.createTailer(i);
        tailers.add(tailer);
        return new ConsumerRunner<>(factory, policy, tailer);
    }

    @Override
    protected String getThreadPrefix() {
        return "Nuxeo-Consumer";
    }

    @Override
    protected void afterCall(List<ConsumerStatus> ret) {
        closeTailers();
        ret.forEach(log::info);
        log.warn(ConsumerStatus.toString(ret));
    }

    private void closeTailers() {
        tailers.stream().filter(Objects::nonNull).forEach(tailer -> {
            try {
                tailer.close();
            } catch (Exception e) {
                log.error("Unable to close tailer: " + tailer.getQueue());
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeTailers();
    }
}

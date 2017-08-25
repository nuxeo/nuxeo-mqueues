package org.nuxeo.lib.core.mqueues.tests.pattern.producer;
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

import org.nuxeo.lib.core.mqueues.pattern.keyValueMessage;
import org.nuxeo.lib.core.mqueues.pattern.producer.AbstractProducer;

/**
 * Produce ordered messages in each queues. This requires to have a number of producer <= nb consumers.
 *
 * @since 9.1
 */
public class OrderedIdMessageProducer extends AbstractProducer<keyValueMessage> {
    protected final long nbMessage;
    protected long count = 0;

    public OrderedIdMessageProducer(int producerId, long nbMessage) {
        super(producerId);
        this.nbMessage = nbMessage;
    }

    @Override
    public int getPartition(keyValueMessage message, int partitions) {
        if (getProducerId() > partitions) {
            throw new IllegalStateException("You should use less producers than consumers to get ordering");
        }
        return getProducerId() % partitions;
    }

    @Override
    public boolean hasNext() {
        return count < nbMessage;
    }

    @Override
    public keyValueMessage next() {
        keyValueMessage ret = keyValueMessage.of(String.valueOf(count));
        count += 1;
        return ret;
    }
}

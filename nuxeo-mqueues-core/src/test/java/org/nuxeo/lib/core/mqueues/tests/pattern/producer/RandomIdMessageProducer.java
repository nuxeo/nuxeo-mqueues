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

import org.nuxeo.lib.core.mqueues.pattern.KeyValueMessage;
import org.nuxeo.lib.core.mqueues.pattern.producer.AbstractProducer;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @since 9.1
 */
public class RandomIdMessageProducer extends AbstractProducer<KeyValueMessage> {

    protected final long nbMessage;
    protected final AtomicLong totalCount = new AtomicLong(0);
    protected long count = 0;

    public RandomIdMessageProducer(int producerId, long nbMessage) {
        super(producerId);
        this.nbMessage = nbMessage;
    }

    @Override
    public int getPartition(KeyValueMessage message, int partitions) {
        // random attribution
        return ThreadLocalRandom.current().nextInt(0, partitions);
    }

    @Override
    public boolean hasNext() {
        return count < nbMessage;
    }

    @Override
    public KeyValueMessage next() {
        KeyValueMessage ret = KeyValueMessage.of("Random message " + totalCount.getAndIncrement());
        count += 1;
        return ret;
    }
}

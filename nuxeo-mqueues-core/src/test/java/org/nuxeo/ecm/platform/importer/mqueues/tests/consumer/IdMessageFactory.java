package org.nuxeo.ecm.platform.importer.mqueues.tests.consumer;/*
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

import org.nuxeo.ecm.platform.importer.mqueues.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;

/**
 * @since 9.1
 */
public class IdMessageFactory implements ConsumerFactory<IdMessage> {
    /**
     * Factory for consumer that do nothing no op
     */
    public static IdMessageFactory NOOP = new IdMessageFactory(ConsumerType.NOOP);
    /**
     * Factory for consumer that raise error randomly
     */
    public static IdMessageFactory BUGGY = new IdMessageFactory(ConsumerType.BUGGY);

    protected enum ConsumerType {NOOP, BUGGY}

    private final ConsumerType type;

    protected IdMessageFactory(ConsumerType type) {
        this.type = type;
    }

    @Override
    public Consumer<IdMessage> createConsumer(int consumerId) {
        switch (type) {
            case BUGGY:
                return new BuggyIdMessageConsumer(consumerId);
            default:
            case NOOP:
                return new NoopIdMessageConsumer(consumerId);
        }
    }
}

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

import org.nuxeo.ecm.platform.importer.mqueues.consumer.AbstractConsumer;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;

/**
 * No operation consumer.
 *
 * @since 9.1
 */
public class NoopIdMessageConsumer extends AbstractConsumer<IdMessage> {

    public NoopIdMessageConsumer(int consumerId) {
        super(consumerId);
    }

    @Override
    public void begin() {

    }

    @Override
    public void accept(IdMessage message) {

    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

}

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

import java.util.concurrent.ThreadLocalRandom;

/**
 * @since 9.1
 */
public class BuggyIdMessageConsumer extends AbstractConsumer<IdMessage> {


    public BuggyIdMessageConsumer(int consumerId) {
        super(consumerId);
    }

    @Override
    public void begin() {
        if (getRandom100() < 2) {
            throw new BuggyException("Failure in begin");
        }

    }

    @Override
    public void accept(IdMessage message) {
        if (getRandom100() < 1) {
            throw new BuggyException("Failure in accept: " + message);
        }
    }

    @Override
    public void commit() {
        if (getRandom100() < 5) {
            throw new BuggyException("Failure in commit");
        }
    }

    @Override
    public void rollback() {
        if (getRandom100() < 1) {
            throw new BuggyException("Failure in rollback");
        }
    }

    private int getRandom100() {
        return ThreadLocalRandom.current().nextInt(100);
    }

}

package org.nuxeo.ecm.platform.importer.mqueues.tests.pattern.producer;/*
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

import org.nuxeo.ecm.platform.importer.mqueues.pattern.keyValueMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerIterator;

/**
 * @since 9.1
 */
public class RandomIdMessageProducerFactory implements ProducerFactory<keyValueMessage> {
    private final long nbDocuments;
    private final ProducerType type;

    public enum ProducerType {DEFAULT, ORDERED}

    public RandomIdMessageProducerFactory(long nbDocuments) {
        this(nbDocuments, ProducerType.DEFAULT);
    }

    public RandomIdMessageProducerFactory(long nbDocuments, ProducerType type) {
        this.nbDocuments = nbDocuments;
        this.type = type;
    }

    @Override
    public ProducerIterator<keyValueMessage> createProducer(int producerId) {
        switch (type) {
            case ORDERED:
                return new OrderedIdMessageProducer(producerId, nbDocuments);
        }
        return new RandomIdMessageProducer(producerId, nbDocuments);
    }

}

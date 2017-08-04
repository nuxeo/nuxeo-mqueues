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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.producer;

import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.BlobInfoFetcher;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.internals.RandomMQBlobInfoFetcher;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobInfoMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.DocumentMessage;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @since 9.1
 */
public class RandomDocumentMessageProducerFactory implements ProducerFactory<DocumentMessage> {
    private final long nbDocuments;
    private final String lang;
    private final int blobSizeKb;
    private final MQManager<BlobInfoMessage> manager;
    private final String mqName;
    private List<List<MQPartition>> defaultAssignments;

    /**
     * Generates random document messages that contains random blob.
     */
    public RandomDocumentMessageProducerFactory(long nbDocuments, String lang, int blobSizeKb) {
        this.nbDocuments = nbDocuments;
        this.lang = lang;
        this.manager = null;
        this.blobSizeKb = blobSizeKb;
        this.mqName = null;
    }

    /**
     * Generates random documents messages that point to existing blobs.
     */
    public RandomDocumentMessageProducerFactory(long nbDocuments, String lang,
                                                MQManager<BlobInfoMessage> manager, String mqBlobInfoName, int nbThreads) {
        this.nbDocuments = nbDocuments;
        this.lang = lang;
        this.manager = manager;
        this.mqName = mqBlobInfoName;
        this.blobSizeKb = 0;
        this.defaultAssignments = getDefaultAssignments(nbThreads);
    }

    private List<List<MQPartition>> getDefaultAssignments(int nbThreads) {
        Map<String, Integer> streams = Collections.singletonMap(mqName, manager.getAppender(mqName).size());
        return KafkaUtils.roundRobinAssignments(nbThreads, streams);
    }

    @Override
    public ProducerIterator<DocumentMessage> createProducer(int producerId) {
        BlobInfoFetcher fetcher = null;
        if (manager != null) {
            MQTailer<BlobInfoMessage> tailer;
            if (manager.supportSubscribe()){
                tailer = manager.subscribe("DocumentMessageProducer", Collections.singletonList(mqName), null);
            } else{
                tailer = manager.createTailer("DocumentMessageProducer", defaultAssignments.get(producerId));
            }
            fetcher = new RandomMQBlobInfoFetcher(tailer);
        }
        return new RandomDocumentMessageProducer(producerId, nbDocuments, lang, fetcher)
                .withBlob(blobSizeKb, false);
    }
}

/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
package org.nuxeo.ecm.platform.importer.mqueues.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.BlobMessageConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.DocumentMessageConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.DocumentMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.RandomDocumentMessageProducerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.RandomStringBlobMessageProducerFactory;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.nio.file.Path;
import java.util.List;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;


@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy("org.nuxeo.ecm.platform.dublincore")
public abstract class TestDocumentImport {

    protected static final Log log = LogFactory.getLog(TestDocumentImport.class);

    public abstract MQManager getManager() throws Exception;

    @Inject
    CoreSession session;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @SuppressWarnings("unchecked")
    @Test
    public void synchronous() throws Exception {
        final int NB_QUEUE = 5;
        final int NB_PRODUCERS = 5;
        final int NB_DOCUMENTS = 2 * 100;
        try (MQManager<DocumentMessage> manager = getManager()) {
            manager.createIfNotExists("document-import", NB_QUEUE);
            ProducerPool<DocumentMessage> producers = new ProducerPool<>("document-import", manager,
                    new RandomDocumentMessageProducerFactory(NB_DOCUMENTS, "en_US", 2), NB_PRODUCERS);
            List<ProducerStatus> ret = producers.start().get();
            assertEquals(NB_PRODUCERS, (long) ret.size());
            assertEquals(NB_PRODUCERS * NB_DOCUMENTS, ret.stream().mapToLong(r -> r.nbProcessed).sum());


            DocumentModel root = session.getRootDocument();
            ConsumerPool<DocumentMessage> consumers = new ConsumerPool<>("document-import", manager,
                    new DocumentMessageConsumerFactory(root.getRepositoryName(), root.getPathAsString()),
                    ConsumerPolicy.BOUNDED);
            List<ConsumerStatus> ret2 = consumers.start().get();
            assertEquals(NB_QUEUE, (long) ret2.size());
            assertEquals(NB_PRODUCERS * NB_DOCUMENTS, ret2.stream().mapToLong(r -> r.committed).sum());
        }
    }


    @SuppressWarnings("unchecked")
    @Test
    public void importBlobFirst() throws Exception {
        final int NB_QUEUE = 5;
        final int NB_PRODUCERS = 5;
        final long NB_BLOBS = 100;
        final long NB_DOCUMENTS = 2 * 100;
        final Path blobInfoPath = folder.newFolder("blob-info").toPath();

        try (MQManager<BlobMessage> manager = getManager()) {
            manager.createIfNotExists("blob", NB_QUEUE);
            ProducerPool<BlobMessage> producers = new ProducerPool<>("blob", manager,
                    new RandomStringBlobMessageProducerFactory(NB_BLOBS, "en_US", 2), NB_PRODUCERS);
            List<ProducerStatus> ret = producers.start().get();
            assertEquals(NB_PRODUCERS, (long) ret.size());
            assertEquals(NB_PRODUCERS * NB_BLOBS, ret.stream().mapToLong(r -> r.nbProcessed).sum());


            // import blobs
            String blobProviderName = "test";
            ConsumerFactory<BlobMessage> factory = new BlobMessageConsumerFactory(blobProviderName, blobInfoPath);
            ConsumerPool<BlobMessage> consumers = new ConsumerPool<>("blob", manager, factory, ConsumerPolicy.BOUNDED);
            List<ConsumerStatus> ret2 = consumers.start().get();
            assertEquals(NB_QUEUE, (long) ret2.size());
            assertEquals(NB_PRODUCERS * NB_BLOBS, ret2.stream().mapToLong(r -> r.committed).sum());

        }

        // generate documents with blob reference
        try (MQManager<DocumentMessage> manager = getManager()) {
            manager.createIfNotExists("document", NB_QUEUE);
            ProducerFactory<DocumentMessage> factory = new RandomDocumentMessageProducerFactory(NB_DOCUMENTS, "en_US", blobInfoPath);
            ProducerPool<DocumentMessage> producers = new ProducerPool<>("document", manager, factory, NB_PRODUCERS);
            List<ProducerStatus> ret = producers.start().get();
            assertEquals(NB_PRODUCERS, (long) ret.size());
            assertEquals(NB_PRODUCERS * NB_DOCUMENTS, ret.stream().mapToLong(r -> r.nbProcessed).sum());

            // import documents without creating blobs
            DocumentModel root = session.getRootDocument();
            ConsumerFactory<DocumentMessage> factory2 = new DocumentMessageConsumerFactory(root.getRepositoryName(), root.getPathAsString());
            ConsumerPool<DocumentMessage> consumers = new ConsumerPool<>("document", manager, factory2, ConsumerPolicy.BOUNDED);
            List<ConsumerStatus> ret2 = consumers.start().get();
            assertEquals(NB_QUEUE, (long) ret2.size());
            assertEquals(NB_PRODUCERS * NB_DOCUMENTS, ret2.stream().mapToLong(r -> r.committed).sum());

        }

    }


}

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
package org.nuxeo.ecm.platform.mqueues.tests.importer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.mqueues.importer.consumer.BlobInfoWriter;
import org.nuxeo.ecm.platform.mqueues.importer.consumer.MQBlobInfoWriter;
import org.nuxeo.ecm.platform.mqueues.importer.consumer.BlobMessageConsumerFactory;
import org.nuxeo.lib.core.mqueues.pattern.consumer.ConsumerFactory;
import org.nuxeo.lib.core.mqueues.pattern.consumer.ConsumerPolicy;
import org.nuxeo.lib.core.mqueues.pattern.consumer.ConsumerPool;
import org.nuxeo.lib.core.mqueues.pattern.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.mqueues.importer.consumer.DocumentMessageConsumerFactory;
import org.nuxeo.ecm.platform.mqueues.importer.message.BlobInfoMessage;
import org.nuxeo.ecm.platform.mqueues.importer.message.BlobMessage;
import org.nuxeo.ecm.platform.mqueues.importer.message.DocumentMessage;
import org.nuxeo.lib.core.mqueues.pattern.producer.ProducerFactory;
import org.nuxeo.lib.core.mqueues.pattern.producer.ProducerPool;
import org.nuxeo.lib.core.mqueues.pattern.producer.ProducerStatus;
import org.nuxeo.ecm.platform.mqueues.importer.producer.RandomDocumentMessageProducerFactory;
import org.nuxeo.ecm.platform.mqueues.importer.producer.RandomStringBlobMessageProducerFactory;
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
    public void twoStepsImport() throws Exception {
        final int NB_QUEUE = 5;
        final short NB_PRODUCERS = 5;
        final int NB_DOCUMENTS = 2 * 100;
        try (MQManager<DocumentMessage> manager = getManager()) {
            // 1. generate documents with blobs
            manager.createIfNotExists("document-import", NB_QUEUE);
            ProducerPool<DocumentMessage> producers = new ProducerPool<>("document-import", manager,
                    new RandomDocumentMessageProducerFactory(NB_DOCUMENTS, "en_US", 2), NB_PRODUCERS);
            List<ProducerStatus> ret = producers.start().get();
            assertEquals(NB_PRODUCERS, (long) ret.size());
            assertEquals(NB_PRODUCERS * NB_DOCUMENTS, ret.stream().mapToLong(r -> r.nbProcessed).sum());

            // 2. import documents
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
    public void fourStepsImport() throws Exception {
        final int NB_QUEUE = 5;
        final short NB_PRODUCERS = 5;
        final long NB_BLOBS = 100;
        final long NB_DOCUMENTS = 2 * 100;
        final Path blobInfoPath = folder.newFolder("blob-info").toPath();

        try (MQManager<BlobMessage> manager = getManager();
             MQManager<BlobInfoMessage> managerBlobInfo = getManager()) {
            manager.createIfNotExists("blob", NB_QUEUE);
            // 1. generates blobs
            ProducerPool<BlobMessage> producers = new ProducerPool<>("blob", manager,
                    new RandomStringBlobMessageProducerFactory(NB_BLOBS, "en_US", 2, "1234"), NB_PRODUCERS);
            List<ProducerStatus> ret = producers.start().get();
            assertEquals(NB_PRODUCERS, (long) ret.size());
            assertEquals(NB_PRODUCERS * NB_BLOBS, ret.stream().mapToLong(r -> r.nbProcessed).sum());


            // 2. import blobs
            String blobProviderName = "test";
            manager.createIfNotExists("blob-info", NB_QUEUE);
            BlobInfoWriter blobInfoWriter = new MQBlobInfoWriter(managerBlobInfo.getAppender("blob-info"));
            ConsumerFactory<BlobMessage> factory = new BlobMessageConsumerFactory(blobProviderName, blobInfoWriter);
            ConsumerPool<BlobMessage> consumers = new ConsumerPool<>("blob", manager, factory, ConsumerPolicy.BOUNDED);
            List<ConsumerStatus> ret2 = consumers.start().get();
            assertEquals(NB_QUEUE, (long) ret2.size());
            assertEquals(NB_PRODUCERS * NB_BLOBS, ret2.stream().mapToLong(r -> r.committed).sum());

        }


        try (MQManager<DocumentMessage> manager = getManager();
             MQManager<BlobInfoMessage> managerBlobInfo = getManager()) {
            manager.createIfNotExists("document", NB_QUEUE);
            // 3. generate documents using blob reference
            ProducerFactory<DocumentMessage> factory = new RandomDocumentMessageProducerFactory(NB_DOCUMENTS, "en_US",
                    managerBlobInfo, "blob-info");
            ProducerPool<DocumentMessage> producers = new ProducerPool<>("document", manager, factory, NB_PRODUCERS);
            List<ProducerStatus> ret = producers.start().get();
            assertEquals(NB_PRODUCERS, (long) ret.size());
            assertEquals(NB_PRODUCERS * NB_DOCUMENTS, ret.stream().mapToLong(r -> r.nbProcessed).sum());

            // 4. import documents without creating blobs
            DocumentModel root = session.getRootDocument();
            ConsumerFactory<DocumentMessage> factory2 = new DocumentMessageConsumerFactory(root.getRepositoryName(), root.getPathAsString());
            ConsumerPool<DocumentMessage> consumers = new ConsumerPool<>("document", manager, factory2, ConsumerPolicy.BOUNDED);
            List<ConsumerStatus> ret2 = consumers.start().get();
            assertEquals(NB_QUEUE, (long) ret2.size());
            assertEquals(NB_PRODUCERS * NB_DOCUMENTS, ret2.stream().mapToLong(r -> r.committed).sum());

        }

    }


}

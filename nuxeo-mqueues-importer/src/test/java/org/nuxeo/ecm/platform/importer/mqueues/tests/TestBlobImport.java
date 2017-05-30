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
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.BatchPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.BlobMessageConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerStatus;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.RandomStringBlobMessageProducerFactory;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
public abstract class TestBlobImport {
    protected static final Log log = LogFactory.getLog(TestBlobImport.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public abstract MQManager getManager() throws Exception;

    @SuppressWarnings("unchecked")
    @Test
    public void randomStringBlob() throws Exception {
        final int NB_QUEUE = 10;
        final int NB_PRODUCERS = 10;
        final int NB_BLOBS = 2 * 1000;

        try (MQManager<BlobMessage> manager = getManager()) {
            try (MQueue<BlobMessage> mQueue = manager.create("blob-import", NB_QUEUE)) {
                ProducerPool<BlobMessage> producers = new ProducerPool<>(mQueue,
                        new RandomStringBlobMessageProducerFactory(NB_BLOBS, "en_US", 1), NB_PRODUCERS);
                List<ProducerStatus> ret = producers.start().get();
                assertEquals(NB_PRODUCERS, (long) ret.size());
                assertEquals(NB_PRODUCERS * NB_BLOBS, ret.stream().mapToLong(r -> r.nbProcessed).sum());
            }

            final Path output = folder.newFolder("blob-info").toPath();
            try (MQueue<BlobMessage> mQueue = manager.open("blob-import")) {
                String blobProviderName = "test";
                ConsumerPool<BlobMessage> consumers = new ConsumerPool<>(mQueue,
                        new BlobMessageConsumerFactory(blobProviderName, output),
                        ConsumerPolicy.builder().batchPolicy(BatchPolicy.NO_BATCH).build());
                List<ConsumerStatus> ret = consumers.start().get();
                assertEquals(NB_QUEUE, (long) ret.size());
                assertEquals(NB_PRODUCERS * NB_BLOBS, ret.stream().mapToLong(r -> r.committed).sum());
            }
        }
    }

}

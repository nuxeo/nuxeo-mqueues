/*
 * (C) Copyright 2015 Nuxeo SA (http://nuxeo.com/) and others.
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
 *     Benoit Delbosc
 */
package org.nuxeo.ecm.platform.importer.mqueues.automation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.platform.importer.mqueues.message.DocumentMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.producer.ProducerPool;
import org.nuxeo.ecm.platform.importer.mqueues.producer.RandomDocumentMessageProducerFactory;

import java.io.File;
import java.nio.file.Paths;

/**
 * @since 9.1
 */
@Operation(id = RandomDocumentProducers.ID, category = Constants.CAT_SERVICES, label = "Produces random blobs", since = "9.1",
        description = "Produces random blobs in a mqueues.")
public class RandomDocumentProducers {
    private static final Log log = LogFactory.getLog(RandomDocumentProducers.class);
    public static final String ID = "MQImporter.runRandomDocumentProducers";
    public static final String DEFAULT_DOC_QUEUE_NAME = "mq-doc";

    @Context
    protected OperationContext ctx;

    @Param(name = "nbDocuments")
    protected Integer nbDocuments;

    @Param(name = "nbThreads", required = false)
    protected Integer nbThreads = 8;

    @Param(name = "avgBlobSizeKB", required = false)
    protected Integer avgBlobSizeKB = 1;

    @Param(name = "lang", required = false)
    protected String lang = "en_US";

    @Param(name = "queuePath", required = false)
    protected String queuePath;

    @Param(name = "blobInfoPath", required = false)
    protected String blobInfoPath;

    @OperationMethod
    public void run() {
        RandomBlobProducers.checkAccess(ctx);
        queuePath = getQueuePath();
        try (MQueues<DocumentMessage> mQueues = CQMQueues.openOrCreate(new File(queuePath), nbThreads)) {
            ProducerPool<DocumentMessage> producers;
            if (blobInfoPath != null) {
                producers = new ProducerPool<>(mQueues,
                        new RandomDocumentMessageProducerFactory(nbDocuments, lang, Paths.get(blobInfoPath)), nbThreads);
            } else {
                producers = new ProducerPool<>(mQueues,
                        new RandomDocumentMessageProducerFactory(nbDocuments, lang, avgBlobSizeKB), nbThreads);
            }
            producers.start().get();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private String getQueuePath() {
        if (queuePath != null && !queuePath.isEmpty()) {
            return queuePath;
        }
        return getDefaultDocumentQueuePath();
    }

    public static String getDefaultDocumentQueuePath() {
        return RandomBlobProducers.getDefaultQueuePath(DEFAULT_DOC_QUEUE_NAME);
    }

}

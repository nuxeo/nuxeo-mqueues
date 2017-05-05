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
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.platform.importer.mqueues.message.BlobMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.producer.ProducerPool;
import org.nuxeo.ecm.platform.importer.mqueues.producer.RandomStringBlobMessageProducerFactory;
import org.nuxeo.runtime.api.Framework;

import java.io.File;
import java.nio.file.Paths;

/**
 * @since 9.1
 */
@Operation(id = RandomBlobProducers.ID, category = Constants.CAT_SERVICES, label = "Produces random blobs", since = "9.1",
        description = "Produces random blobs in a mqueues.")
public class RandomBlobProducers {
    private static final Log log = LogFactory.getLog(RandomBlobProducers.class);
    public static final String ID = "MQImporter.runRandomBlobProducers";
    public static final String DEFAULT_BLOB_QUEUE_NAME = "mq-blob";

    @Context
    protected OperationContext ctx;

    @Param(name = "nbBlobs")
    protected Integer nbBlobs;

    @Param(name = "nbThreads", required = false)
    protected Integer nbThreads = 8;

    @Param(name = "avgBlobSizeKB", required = false)
    protected Integer avgBlobSizeKB = 1;

    @Param(name = "lang", required = false)
    protected String lang = "en_US";

    @Param(name = "queuePath", required = false)
    protected String queuePath;

    @OperationMethod
    public void run() {
        checkAccess(ctx);
        queuePath = getQueuePath();
        try (MQueues<BlobMessage> mQueues = new CQMQueues<>(new File(queuePath), nbThreads)) {
            ProducerPool<BlobMessage> producers = new ProducerPool<>(mQueues,
                    new RandomStringBlobMessageProducerFactory(nbBlobs, lang, avgBlobSizeKB), nbThreads);
            producers.start().get();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public String getQueuePath() {
        if (queuePath != null && !queuePath.isEmpty()) {
            return queuePath;
        }
        return getDefaultBlobQueuePath();
    }

    public static String getDefaultBlobQueuePath() {
        return getDefaultQueuePath(DEFAULT_BLOB_QUEUE_NAME);
    }

    public static String getDefaultQueuePath(String name) {
        return Paths.get(Framework.getRuntime().getHome().toString(), "tmp", name).toString();
    }

    public static void checkAccess(OperationContext context) {
        NuxeoPrincipal principal = (NuxeoPrincipal) context.getPrincipal();
        if (principal == null || !principal.isAdministrator()) {
            throw new RuntimeException("Unauthorized access: " + principal);
        }
    }


}

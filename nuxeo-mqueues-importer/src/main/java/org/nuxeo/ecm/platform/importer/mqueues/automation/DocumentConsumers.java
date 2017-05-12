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

import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.BatchPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.DocumentConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.DocumentMessageConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.DocumentMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @since 9.1
 */
@Operation(id = DocumentConsumers.ID, category = Constants.CAT_SERVICES, label = "Imports document", since = "9.1",
        description = "Import mqueues document into repository.")
public class DocumentConsumers {
    private static final Log log = LogFactory.getLog(DocumentConsumers.class);
    public static final String ID = "MQImporter.runDocumentConsumers";

    @Context
    protected OperationContext ctx;

    @Param(name = "rootFolder")
    protected String rootFolder;

    @Param(name = "repositoryName", required = false)
    protected String repositoryName;

    @Param(name = "batchSize", required = false)
    protected Integer batchSize = 10;

    @Param(name = "batchThresholdS", required = false)
    protected Integer batchThresholdS = 20;

    @Param(name = "retryMax", required = false)
    protected Integer retryMax = 3;

    @Param(name = "retryDelayS", required = false)
    protected Integer retryDelayS = 2;

    @Param(name = "queuePath", required = false)
    protected String queuePath;

    @OperationMethod
    public void run() {
        RandomBlobProducers.checkAccess(ctx);
        queuePath = getQueuePath();
        repositoryName = getRepositoryName();
        try (MQueues<DocumentMessage> mQueues = CQMQueues.open(new File(queuePath))) {
            DocumentConsumerPool<DocumentMessage> consumers = new DocumentConsumerPool<>(mQueues,
                    new DocumentMessageConsumerFactory(repositoryName, rootFolder),
                    ConsumerPolicy.builder()
                            .batchPolicy(BatchPolicy.builder().capacity(batchSize)
                                    .timeThreshold(Duration.ofSeconds(batchThresholdS))
                                    .build())
                            .retryPolicy(
                                    new RetryPolicy().withMaxRetries(retryMax).withDelay(retryDelayS, TimeUnit.SECONDS))
                            .salted()
                            .build());
            consumers.start().get();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private String getRepositoryName() {
        if (repositoryName != null && !repositoryName.isEmpty()) {
            return repositoryName;
        }
        return ctx.getCoreSession().getRepositoryName();
    }

    private String getQueuePath() {
        if (queuePath != null && !queuePath.isEmpty()) {
            return queuePath;
        }
        return RandomDocumentProducers.getDefaultDocumentQueuePath();
    }

}

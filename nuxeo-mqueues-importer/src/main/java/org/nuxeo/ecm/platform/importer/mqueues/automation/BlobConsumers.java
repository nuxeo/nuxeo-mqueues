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
import org.nuxeo.ecm.platform.importer.mqueues.consumer.BlobMessageConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.message.BlobMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @since 9.1
 */
@Operation(id = BlobConsumers.ID, category = Constants.CAT_SERVICES, label = "Import blobs", since = "9.1",
        description = "Import mqueues blob into the binarystore.")
public class BlobConsumers {
    private static final Log log = LogFactory.getLog(BlobConsumers.class);
    public static final String ID = "MQImporter.runBlobConsumers";

    @Context
    protected OperationContext ctx;

    @Param(name = "blobInfoPath")
    protected String blobInfoPath;

    @Param(name = "blobProviderName", required = false)
    protected String blobProviderName = "default";

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
        try (MQueues<BlobMessage> mQueues = new CQMQueues<>(new File(queuePath))) {
            ConsumerPool<BlobMessage> consumers = new ConsumerPool<>(mQueues,
                    new BlobMessageConsumerFactory(blobProviderName, Paths.get(blobInfoPath)),
                    ConsumerPolicy.builder()
                            .batchPolicy(BatchPolicy.builder().capacity(batchSize)
                                    .timeThreshold(Duration.ofSeconds(batchThresholdS)).build())
                            .retryPolicy(new RetryPolicy().withMaxRetries(retryMax).withDelay(retryDelayS, TimeUnit.SECONDS)).build());
            consumers.start().get();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private String getQueuePath() {
        if (queuePath != null && !queuePath.isEmpty()) {
            return queuePath;
        }
        return RandomBlobProducers.getDefaultBlobQueuePath();
    }


}

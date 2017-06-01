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
import org.nuxeo.ecm.platform.importer.mqueues.chronicle.ChronicleConfig;
import org.nuxeo.ecm.platform.importer.mqueues.kafka.KafkaConfigService;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle.ChronicleMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.BatchPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.BlobMessageConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobMessage;
import org.nuxeo.runtime.api.Framework;

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

    @Param(name = "mqName", required = false)
    protected String mqName;

    @Param(name = "kafkaConfig", required = false)
    protected String kafkaConfig;

    @OperationMethod
    public void run() {
        RandomBlobProducers.checkAccess(ctx);
        try (MQManager<BlobMessage> manager = getManager()) {
            ConsumerPool<BlobMessage> consumers = new ConsumerPool<>(getMQName(), manager,
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

    protected String getMQName() {
        if (mqName != null) {
            return mqName;
        }
        return RandomBlobProducers.DEFAULT_MQ_NAME;
    }

    protected MQManager<BlobMessage> getManager() {
        if (kafkaConfig == null || kafkaConfig.isEmpty()) {
            return new ChronicleMQManager<>(ChronicleConfig.getBasePath("import"));
        }
        KafkaConfigService service = Framework.getService(KafkaConfigService.class);
        return new KafkaMQManager<>(service.getZkServers(kafkaConfig),
                service.getTopicPrefix(kafkaConfig),
                service.getProducerProperties(kafkaConfig),
                service.getConsumerProperties(kafkaConfig));
    }

}

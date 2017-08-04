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
import org.nuxeo.ecm.platform.importer.mqueues.chronicle.ChronicleConfig;
import org.nuxeo.ecm.platform.importer.mqueues.kafka.KafkaConfigService;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRebalanceListener;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle.ChronicleMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobInfoMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.DocumentMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.ProducerPool;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.producer.RandomDocumentMessageProducerFactory;
import org.nuxeo.runtime.api.Framework;

import java.io.Externalizable;
import java.util.Collection;


/**
 * @since 9.1
 */
@Operation(id = RandomDocumentProducers.ID, category = Constants.CAT_SERVICES, label = "Produces random blobs", since = "9.1",
        description = "Produces random blobs in a mqueues.")
public class RandomDocumentProducers implements MQRebalanceListener {
    private static final Log log = LogFactory.getLog(RandomDocumentProducers.class);
    public static final String ID = "MQImporter.runRandomDocumentProducers";
    public static final String DEFAULT_MQ_NAME = "mq-doc";

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

    @Param(name = "mqName", required = false)
    protected String mqName;

    @Param(name = "mqSize", required = false)
    protected Integer mqSize;

    @Param(name = "mqBlobInfo", required = false)
    protected String mqBlobInfoName;

    @Param(name = "kafkaConfig", required = false)
    protected String kafkaConfig;

    @OperationMethod
    public void run() {
        RandomBlobProducers.checkAccess(ctx);
        try (MQManager<DocumentMessage> manager = getManager();
             MQManager<BlobInfoMessage> managerBlobInfo = getManager()) {
            manager.createIfNotExists(getMQName(), getMQSize());
            ProducerPool<DocumentMessage> producers;
            if (mqBlobInfoName != null) {
                producers = new ProducerPool<>(getMQName(), manager,
                        new RandomDocumentMessageProducerFactory(nbDocuments, lang, managerBlobInfo, mqBlobInfoName, nbThreads),
                        nbThreads.shortValue());
            } else {
                producers = new ProducerPool<>(getMQName(), manager,
                        new RandomDocumentMessageProducerFactory(nbDocuments, lang, avgBlobSizeKB), nbThreads.shortValue());
            }
            producers.start().get();
            producers.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected int getMQSize() {
        if (mqSize != null && mqSize > 0) {
            return mqSize;
        }
        return nbThreads;
    }

    protected String getMQName() {
        if (mqName != null) {
            return mqName;
        }
        return DEFAULT_MQ_NAME;
    }


    protected <T extends Externalizable> MQManager<T> getManager() {
        if (kafkaConfig == null || kafkaConfig.isEmpty()) {
            return new ChronicleMQManager<>(ChronicleConfig.getBasePath("import"),
                    ChronicleConfig.getRetentionDuration());
        }
        KafkaConfigService service = Framework.getService(KafkaConfigService.class);
        return new KafkaMQManager<>(service.getZkServers(kafkaConfig),
                service.getTopicPrefix(kafkaConfig),
                service.getProducerProperties(kafkaConfig),
                service.getConsumerProperties(kafkaConfig));
    }

    @Override
    public void onPartitionsRevoked(Collection<MQPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<MQPartition> partitions) {

    }
}

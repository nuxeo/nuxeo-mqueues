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
package org.nuxeo.ecm.platform.mqueues.importer.automation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.platform.mqueues.MQService;
import org.nuxeo.ecm.platform.mqueues.importer.message.BlobMessage;
import org.nuxeo.ecm.platform.mqueues.importer.producer.RandomStringBlobMessageProducerFactory;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.pattern.producer.ProducerPool;
import org.nuxeo.runtime.api.Framework;

import static org.nuxeo.ecm.platform.mqueues.importer.automation.BlobConsumers.DEFAULT_MQ_CONFIG;

/**
 * @since 9.1
 */
@Operation(id = RandomBlobProducers.ID, category = Constants.CAT_SERVICES, label = "Produces random blobs", since = "9.1",
        description = "Produces random blobs in a mqueues.")
public class RandomBlobProducers {
    private static final Log log = LogFactory.getLog(RandomBlobProducers.class);
    public static final String ID = "MQImporter.runRandomBlobProducers";
    public static final String DEFAULT_MQ_NAME = "mq-blob";

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

    @Param(name = "mqName", required = false)
    protected String mqName;

    @Param(name = "mqSize", required = false)
    protected Integer mqSize;

    @Param(name = "mqConfig", required = false)
    protected String mqConfig;

    @Param(name = "blobMarker", required = false)
    protected String blobMarker;


    @OperationMethod
    public void run() {
        checkAccess(ctx);
        MQService service = Framework.getService(MQService.class);
        MQManager manager = service.getManager(getMQConfig());
        try {
            manager.createIfNotExists(getMQName(), getMQSize());
            try (ProducerPool<BlobMessage> producers = new ProducerPool<>(getMQName(), manager,
                    new RandomStringBlobMessageProducerFactory(nbBlobs, lang, avgBlobSizeKB, blobMarker),
                    nbThreads.shortValue())) {
                producers.start().get();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected String getMQName() {
        if (mqName != null) {
            return mqName;
        }
        return DEFAULT_MQ_NAME;
    }

    protected int getMQSize() {
        if (mqSize != null && mqSize > 0) {
            return mqSize;
        }
        return nbThreads;
    }

    protected static void checkAccess(OperationContext context) {
        NuxeoPrincipal principal = (NuxeoPrincipal) context.getPrincipal();
        if (principal == null || !principal.isAdministrator()) {
            throw new RuntimeException("Unauthorized access: " + principal);
        }
    }

    protected String getMQConfig() {
        if (mqConfig != null) {
            return mqConfig;
        }
        return DEFAULT_MQ_CONFIG;
    }
}

/*
 * (C) Copyright 2017 Nuxeo SA (http://nuxeo.com/) and others.
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

package org.nuxeo.ecm.platform.mqueues.workmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.lib.core.mqueues.computation.Record;
import org.nuxeo.ecm.platform.mqueues.kafka.KafkaConfigService;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.runtime.api.Framework;


/**
 * @since 9.2
 */
public class WorkManagerComputationKafka extends WorkManagerComputation {
    protected static final Log log = LogFactory.getLog(WorkManagerComputationKafka.class);
    public static final String NUXEO_WORKMANAGER_KAFKA_CONFIG_PROP = "nuxeo.mqueue.work.kafka.config";
    public static final String NUXEO_WORKMANAGER_KAFKA_OVERPROVISIONING_PROP = "nuxeo.mqueue.work.kafka.overprovisioning";
    public static final String DEFAULT_CONFIG = "default";
    public static final String DEFAULT_OVERPROVISIONING = "3";

    @Override
    protected MQManager<Record> initStream() {
        KafkaConfigService service = Framework.getService(KafkaConfigService.class);
        String kafkaConfig = Framework.getProperty(NUXEO_WORKMANAGER_KAFKA_CONFIG_PROP, DEFAULT_CONFIG);
        log.info("Init WorkManagerComputation with Kafka, using configuration: " + kafkaConfig);
        return new KafkaMQManager<>(service.getZkServers(kafkaConfig),
                service.getTopicPrefix(kafkaConfig),
                service.getProducerProperties(kafkaConfig),
                service.getConsumerProperties(kafkaConfig));
    }

    @Override
    protected int getOverProvisioningFactor() {
        return Integer.valueOf(Framework.getProperty(NUXEO_WORKMANAGER_KAFKA_OVERPROVISIONING_PROP,
                DEFAULT_OVERPROVISIONING));
    }
}

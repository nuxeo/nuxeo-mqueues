/*
 * (C) Copyright 2006-2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 *
 * Contributors:
 *     anechaev
 */
package org.nuxeo.ecm.platform.importer.mqueues.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.event.EventServiceComponent;
import org.nuxeo.runtime.model.ComponentContext;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.DefaultComponent;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaConfigServiceImpl extends DefaultComponent implements KafkaConfigService {
    private static final Log log = LogFactory.getLog(KafkaConfigServiceImpl.class);
    public static final String KAFKA_CONFIG_XP = "kafkaConfig";
    protected Map<String, KafkaConfigDescriptor> configs = new HashMap<>();

    @Override
    public void registerContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
        if (extensionPoint.equals(KAFKA_CONFIG_XP)) {
            KafkaConfigDescriptor descriptor = (KafkaConfigDescriptor) contribution;
            configs.put(descriptor.name, descriptor);
            log.info(String.format("Register Kafka contribution: %s", descriptor.name));
        }
    }

    @Override
    public int getApplicationStartedOrder() {
        // since there is no deps, let's start before WorkManager
        return EventServiceComponent.APPLICATION_STARTED_ORDER - 10;
    }

    @Override
    public void deactivate(ComponentContext context) {
        super.deactivate(context);
        log.debug("Deactivating service");
    }

    @Override
    public void activate(ComponentContext context) {
        super.activate(context);
        log.debug("Activating service");
    }

    public Set<String> getConfigNames() {
        return configs.keySet();
    }

    @Override
    public String getZkServers(String configName) {
        checkConfigName(configName);
        return configs.get(configName).zkServers;
    }

    protected void checkConfigName(String configName) {
        if (!configs.containsKey(configName)) {
            throw new IllegalArgumentException("Unknown configuration name: " + configName);
        }
    }

    @Override
    public Properties getProducerProperties(String configName) {
        checkConfigName(configName);
        return configs.get(configName).getProducerProperties();
    }

    @Override
    public Properties getConsumerProperties(String configName) {
        checkConfigName(configName);
        return configs.get(configName).getConsumerProperties();
    }

    @Override
    public String getTopicPrefix(String configName) {
        checkConfigName(configName);
        return configs.get(configName).getTopicPrefix();
    }
}

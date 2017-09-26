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
package org.nuxeo.ecm.platform.mqueues;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.Environment;
import org.nuxeo.ecm.core.event.EventServiceComponent;
import org.nuxeo.ecm.platform.mqueues.kafka.KafkaConfigService;
import org.nuxeo.lib.core.mqueues.computation.ComputationManager;
import org.nuxeo.lib.core.mqueues.computation.Settings;
import org.nuxeo.lib.core.mqueues.computation.Topology;
import org.nuxeo.lib.core.mqueues.computation.mqueue.MQComputationManager;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.chronicle.ChronicleMQManager;
import org.nuxeo.lib.core.mqueues.mqueues.kafka.KafkaMQManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.model.ComponentContext;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.ComponentManager;
import org.nuxeo.runtime.model.ComponentManager.LifeCycleHandler;
import org.nuxeo.runtime.model.DefaultComponent;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 9.3
 */
public class MQServiceImpl extends DefaultComponent implements MQService {
    private static final Log log = LogFactory.getLog(MQServiceImpl.class);
    public static final String NUXEO_MQUEUE_DIR_PROP = "nuxeo.mqueue.chronicle.dir";
    public static final String NUXEO_MQUEUE_RET_DURATION_PROP = "nuxeo.mqueue.chronicle.retention.duration";
    protected static final String MQ_CONFIG_XP = "config";
    protected static final String MQ_TOPOLOGY_XP = "topology";
    protected Map<String, ConfigDescriptor> configs = new HashMap<>();
    protected Map<String, MQManager> managers = new HashMap<>();
    protected Map<String, ComputationManager> computationManagers = new HashMap<>();
    protected Map<String, TopologyDescriptor> topologies = new HashMap<>();

    @Override
    public int getApplicationStartedOrder() {
        return EventServiceComponent.APPLICATION_STARTED_ORDER - 20;
    }

    @Override
    public MQManager getManager(String name) {
        if (!managers.containsKey(name)) {
            if (!configs.containsKey(name)) {
                throw new IllegalArgumentException("Unknown MQ configuration: " + name);
            }
            ConfigDescriptor config = configs.get(name);
            if (config.getType() == "kafka") {
                managers.put(name, createKafkaMQManager(config));
            } else {
                managers.put(name, createChronicleMQManager(config));
            }
        }
        return managers.get(name);
    }

    protected MQManager createKafkaMQManager(ConfigDescriptor config) {
        String kafkaConfig = config.getOption("config", "default");
        KafkaConfigService service = Framework.getService(KafkaConfigService.class);
        return new KafkaMQManager(service.getZkServers(kafkaConfig),
                service.getTopicPrefix(kafkaConfig),
                service.getProducerProperties(kafkaConfig),
                service.getConsumerProperties(kafkaConfig));
    }

    protected MQManager createChronicleMQManager(ConfigDescriptor config) {
        String basePath = config.getOption("basePath", null);
        String directory = config.getOption("directory", config.getName());
        Path path = getChroniclePath(basePath, directory);
        String retention = getChronicleRetention(config.getOption("retention", null));
        return new ChronicleMQManager(path, retention);
    }

    protected String getChronicleRetention(String retention) {
        return retention != null ? retention : Framework.getProperty(NUXEO_MQUEUE_RET_DURATION_PROP, "4d");
    }

    protected Path getChroniclePath(String basePath, String name) {
        if (basePath != null) {
            return Paths.get(basePath, name).toAbsolutePath();
        }
        basePath = Framework.getProperty(NUXEO_MQUEUE_DIR_PROP);
        if (basePath != null) {
            return Paths.get(basePath, name).toAbsolutePath();
        }
        basePath = Framework.getProperty(Environment.NUXEO_DATA_DIR);
        if (basePath != null) {
            return Paths.get(basePath, "mqueue", name).toAbsolutePath();
        }
        return Paths.get(Framework.getRuntime().getHome().getAbsolutePath(),
                "data", "mqueue", name).toAbsolutePath();
    }

    protected void createMQueueIfNotExists(String name, ConfigDescriptor config) {
        if (config.getMQueuesToCreate().isEmpty()) {
            return;
        }
        MQManager manager = getManager(name);
        config.getMQueuesToCreate()
                .forEach((mqName, size) -> {
                    log.info("Create if not exists MQ: " + mqName + " with manager: " + name);
                    manager.createIfNotExists(mqName, size);
                });
    }

    @Override
    public void start(ComponentContext context) {
        super.start(context);
        configs.forEach(this::createMQueueIfNotExists);
        Framework.getRuntime().getComponentManager().addListener(new ComponentsLifeCycleListener());
    }

    protected void startComputations() {
        topologies.forEach(this::startComputations);
    }

    protected void startComputations(String name, TopologyDescriptor descriptor) {
        if (computationManagers.containsKey(name)) {
            log.error("Computation topology already started: " + name);
            return;
        }
        log.info("Starting computation topology: " + name + " with manager: " + descriptor.config);
        MQManager manager = getManager(descriptor.config);
        Topology topology;
        try {
            topology = descriptor.klass.newInstance().getTopology();
        } catch (InstantiationException | IllegalAccessException e) {
            log.error("Can not create topology for " + name, e);
            return;
        }
        ComputationManager computationManager = new MQComputationManager(manager);
        Settings settings = new Settings(descriptor.defaultConcurrency, descriptor.defaultPartitions);
        descriptor.computations.forEach(comp -> settings.setConcurrency(comp.name, comp.concurrency));
        descriptor.streams.forEach(stream -> settings.setPartitions(stream.name, stream.partitions));
        if (log.isDebugEnabled()) {
            log.debug("Starting computation topology: " + name + "\n" + topology.toPlantuml(settings));
        }
        computationManager.start(topology, settings);
        computationManagers.put(name, computationManager);

    }

    @Override
    public void stop(ComponentContext context) throws InterruptedException {
        super.stop(context);
        stopComputations(); // should have already be done by the beforeStop listener
        closeManagers();
    }

    protected void stopComputations() {
        computationManagers.forEach((name, manager) -> {
            manager.stop(Duration.ofSeconds(1));
        });
        computationManagers.clear();
    }

    protected void closeManagers() {
        managers.forEach((name, manager) -> {
            try {
                manager.close();
            } catch (Exception e) {
                log.warn("Failed to close MQManager: " + name, e);
            }
        });
        managers.clear();
    }

    @Override
    public void registerContribution(Object contribution, String extensionPoint, ComponentInstance contributor) {
        if (extensionPoint.equals(MQ_CONFIG_XP)) {
            ConfigDescriptor descriptor = (ConfigDescriptor) contribution;
            configs.put(descriptor.name, descriptor);
            log.info(String.format("Register MQ Config: %s", descriptor.name));
        } else if (extensionPoint.equals(MQ_TOPOLOGY_XP)) {
            TopologyDescriptor descriptor = (TopologyDescriptor) contribution;
            topologies.put(descriptor.name, descriptor);
            log.info(String.format("Register MQ Topology: %s", descriptor.name));
        }
    }

    protected class ComponentsLifeCycleListener extends LifeCycleHandler {
        @Override
        public void afterStart(ComponentManager mgr, boolean isResume) {
            // this is called once all components are started and ready
            startComputations();
        }

        @Override
        public void beforeStop(ComponentManager mgr, boolean isStandby) {
            // this is called before components are stopped
            stopComputations();
        }
    }
}

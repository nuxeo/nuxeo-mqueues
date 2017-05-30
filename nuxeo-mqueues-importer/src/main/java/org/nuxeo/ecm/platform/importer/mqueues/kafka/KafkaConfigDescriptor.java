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

import org.nuxeo.common.xmap.annotation.XNode;
import org.nuxeo.common.xmap.annotation.XNodeList;
import org.nuxeo.common.xmap.annotation.XNodeMap;
import org.nuxeo.common.xmap.annotation.XObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@XObject("kafkaConfig")
public class KafkaConfigDescriptor {
    private static final String RANDOM_TOPIC_PREFIX = "RANDOM()";

    @XNode("@name")
    public String name;

    @XNode("@zkServers")
    public String zkServers;

    @XNode("@topicPrefix")
    public String topicPrefix;

    @XNode("producerProperties")
    protected ProducerProperties producerProperties;

    @XNode("consumerProperties")
    protected ConsumerProperties consumerProperties;


    public Properties getProducerProperties() {
        if (producerProperties == null) {
            return new Properties();
        }
        return producerProperties.properties;
    }

    public Properties getConsumerProperties() {
        if (consumerProperties == null) {
            return new Properties();
        }
        return consumerProperties.properties;
    }

    public String getTopicPrefix() {
        if (topicPrefix == null) {
            return "";
        }
        if (RANDOM_TOPIC_PREFIX.equals(topicPrefix)) {
            topicPrefix = "nuxeo-test-" + System.currentTimeMillis() + "-";
        }
        return topicPrefix;
    }

    @XObject("producerProperties")
    public static class ProducerProperties {
        @XNodeMap(value = "property", key = "@name", type = Properties.class, componentType = String.class)
        protected Properties properties = new Properties();
    }

    @XObject("consumerProperties")
    public static class ConsumerProperties {
        @XNodeMap(value = "property", key = "@name", type = Properties.class, componentType = String.class)
        protected Properties properties = new Properties();
    }
}

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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka;

import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;

import java.io.Externalizable;
import java.util.Properties;

/**
 * @since 9.2
 */
public class KafkaMQManager<M extends Externalizable> extends MQManager<M> {
    private final KafkaUtils kUtils;
    private final Properties producerProperties;
    private final Properties consumerProperties;
    private final String prefix;


    public KafkaMQManager(String zkServers, Properties producerProperties, Properties consumerProperties) {
        this(zkServers, null, producerProperties, consumerProperties);
    }

    public KafkaMQManager(String zkServers, String topicPrefix, Properties producerProperties, Properties consumerProperties) {
        this.prefix = (topicPrefix != null) ? topicPrefix : "";
        this.kUtils = new KafkaUtils(zkServers);
        this.producerProperties = producerProperties;
        this.consumerProperties = consumerProperties;
    }

    protected String getTopicName(String name) {
        return prefix + name;
    }

    @Override
    public boolean exists(String name) {
        return kUtils.topicExists(getTopicName(name));
    }


    @Override
    public MQueue<M> open(String name) {
        return KafkaMQueue.open(getTopicName(name), name, producerProperties, consumerProperties);
    }

    @Override
    public MQueue<M> create(String name, int size) {
        kUtils.createTopic(getTopicName(name), size);
        return KafkaMQueue.open(getTopicName(name), name, producerProperties, consumerProperties);
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public Properties getConsumerProperties() {
        return consumerProperties;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (kUtils != null) {
            kUtils.close();
        }
    }

}

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
package org.nuxeo.ecm.platform.importer.mqueues.streams.mqueues;

import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Streams;

import java.util.Properties;

/**
 * @since 9.2
 */
public class StreamsMQKafka extends Streams {
    private final KafkaUtils kUtils;
    private final Properties producerProperties;
    private final Properties consumerProperties;
    private final String prefix;

    public StreamsMQKafka(String zkServers, Properties producerProperties, Properties consumerProperties) {
        this(zkServers, null, producerProperties, consumerProperties);
    }

    public StreamsMQKafka(String zkServers, String topicPrefix, Properties producerProperties, Properties consumerProperties) {
        this.prefix = (topicPrefix != null) ? topicPrefix : "";
        this.kUtils = new KafkaUtils(zkServers);
        this.producerProperties = producerProperties;
        this.consumerProperties = consumerProperties;
    }

    @Override
    public boolean exists(String name) {
        return kUtils.topicExists(prefix + name);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (kUtils != null) {
            kUtils.close();
        }
    }

    @Override
    public Stream open(String name) {
        return StreamMQKafka.open(prefix + name, name, producerProperties, consumerProperties);
    }

    @Override
    public Stream create(String name, int partitions) {
        return StreamMQKafka.create(prefix + name, name, producerProperties, consumerProperties, partitions);
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }
}

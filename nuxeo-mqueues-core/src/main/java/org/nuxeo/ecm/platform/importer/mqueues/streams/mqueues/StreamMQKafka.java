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

import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KMQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Record;

import java.util.Properties;

/**
 * @since 9.2
 */
public class StreamMQKafka extends StreamMQ {

    public static StreamMQKafka create(String topic, String name, Properties producerProperties, Properties consumerProperties, int partitions) {
        return new StreamMQKafka(topic, name, producerProperties, consumerProperties, partitions);
    }

    public static StreamMQKafka open(String topic, String name, Properties producerProperties, Properties consumerProperties) {
        return new StreamMQKafka(topic, name, producerProperties, consumerProperties);
    }

    protected StreamMQKafka(String topic, String name, Properties consumerProperties, Properties producerProperties, int partitions) {
        super(createTopic(topic, consumerProperties, producerProperties, partitions), name, partitions);
    }

    protected static MQueues<Record> createTopic(String topic, Properties producerProperties, Properties consumerProperties, int partitions) {
        try (KafkaUtils kafka = new KafkaUtils()) {
            kafka.createTopic(topic, partitions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return KMQueues.open(topic, producerProperties, consumerProperties);
    }

    protected StreamMQKafka(String topic, String name, Properties producerProperties, Properties consumerProperties) {
        super(KMQueues.open(topic, producerProperties, consumerProperties), name, 0);
    }

}

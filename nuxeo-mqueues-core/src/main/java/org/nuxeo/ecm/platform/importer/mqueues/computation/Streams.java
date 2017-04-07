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
package org.nuxeo.ecm.platform.importer.mqueues.computation;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A set of {@link Stream}.
 *
 * @since 9.1
 */
public class Streams {
    private final StreamFactory factory;
    private final Map<String, Stream> streams = new ConcurrentHashMap<>();

    public Streams(StreamFactory factory) {
        this.factory = factory;
    }

    public Stream getStream(String streamName) {
        return streams.get(streamName);
    }

    public synchronized Stream getOrCreateStream(String name, int partitions) {
        if (!streams.containsKey(name)) {
            streams.put(name, factory.createStream(name, partitions));
        }
        return getStream(name);
    }
}

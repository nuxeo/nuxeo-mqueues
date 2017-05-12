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

import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;

import java.io.File;
import java.nio.file.Path;

/**
 * @since 9.2
 */
public class StreamMQChronicle extends StreamMQ {

    public static StreamMQChronicle create(Path basePath, String name, int partitions) {
        return new StreamMQChronicle(basePath, name, partitions);
    }

    public static StreamMQChronicle open(Path basePath, String name) {
        return new StreamMQChronicle(basePath, name);
    }

    protected StreamMQChronicle(Path basePath, String name, int partitions) {
        super(CQMQueues.create(new File(basePath.toFile(), name), partitions), name, partitions);
    }

    protected StreamMQChronicle(Path basePath, String name) {
        super(CQMQueues.open(new File(basePath.toFile(), name)), name, 0);
    }

}

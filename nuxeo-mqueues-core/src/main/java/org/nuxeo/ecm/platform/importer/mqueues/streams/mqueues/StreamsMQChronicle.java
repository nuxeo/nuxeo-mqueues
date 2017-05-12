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
import org.nuxeo.ecm.platform.importer.mqueues.streams.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Streams;

import java.io.File;
import java.nio.file.Path;

/**
 * @since 9.2
 */
public class StreamsMQChronicle extends Streams {
    private final Path basePath;

    public StreamsMQChronicle(Path basePath) {
        this.basePath = basePath;
    }

    @Override
    public boolean exists(String name) {
        return CQMQueues.exists(new File(basePath.toFile(), name));
    }

    @Override
    public Stream open(String name) {
        return StreamMQChronicle.open(basePath, name);
    }

    @Override
    public Stream create(String name, int partitions) {
        return StreamMQChronicle.create(basePath, name, partitions);
    }
}

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
package org.nuxeo.ecm.platform.importer.mqueues.tests.computation;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Streams;
import org.nuxeo.ecm.platform.importer.mqueues.streams.mqueues.StreamsMQChronicle;

import java.io.File;
import java.io.IOException;

/**
 * @since 9.1
 */
public class TestStreamChronicle extends TestStream {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private File basePath;

    @Override
    public Streams getStreams() throws Exception {
        this.basePath = folder.newFolder();
        return new StreamsMQChronicle(basePath.toPath());
    }

    @Override
    public Streams getSameStreams() throws IOException {
        return new StreamsMQChronicle(basePath.toPath());
    }
}

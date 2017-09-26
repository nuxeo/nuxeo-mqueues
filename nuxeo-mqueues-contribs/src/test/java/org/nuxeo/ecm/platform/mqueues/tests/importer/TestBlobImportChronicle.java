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

package org.nuxeo.ecm.platform.mqueues.tests.importer;

import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.chronicle.ChronicleMQManager;

import java.nio.file.Path;

/**
 * @since 9.2
 */
public class TestBlobImportChronicle extends TestBlobImport {
    protected Path basePath;

    @Override
    public MQManager getManager() throws Exception {
        if (basePath == null) {
            basePath = folder.newFolder("mqueue").toPath();
        }
        return new ChronicleMQManager(basePath);
    }
}

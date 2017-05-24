/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 */
package org.nuxeo.ecm.platform.importer.mqueues.tests.pattern;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.ChronicleMQManager;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;

public class TestPatternQueuingChronicle extends TestPatternQueuing {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Override
    public MQManager<IdMessage> createManager() throws Exception {
        return new ChronicleMQManager<>(folder.newFolder().toPath());
    }
}
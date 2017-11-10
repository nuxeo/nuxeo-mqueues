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
package org.nuxeo.ecm.platform.mqueues.tests.tools;


import org.junit.Before;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.chronicle.ChronicleMQManager;

import java.io.File;
import java.nio.file.Path;

import static org.nuxeo.ecm.platform.mqueues.tests.importer.TestAutomationChronicle.IS_WIN;

public class TestDebugToolsChronicle extends TestDebugTools {
    public static final String BASE_PATH = "/tmp/nuxeo-server-tomcat-9.3-SNAPSHOT/nxserver/data/mqueue/work";

    @Before
    public void skipWindowsThatDontCleanTempFolder() {
        org.junit.Assume.assumeFalse(IS_WIN);
    }

    @Override
    public MQManager createManager() {
        Path basePath = new File(BASE_PATH).getAbsoluteFile().toPath();
        return new ChronicleMQManager(basePath);
    }

}

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
package org.nuxeo.ecm.platform.importer.mqueues.tests;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;

import java.io.File;

public class TestPatternQueuingChronicle extends TestPatternQueuing {
    private File basePath;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public
    TestName name = new TestName();

    @Override
    public MQueues<IdMessage> createMQ(int partitions) throws Exception {
        basePath = folder.newFolder(name.getMethodName());
        if (CQMQueues.exists(basePath)) {
            CQMQueues.delete(basePath);
        }
        return CQMQueues.create(basePath, partitions);
    }

    @Override
    public MQueues<IdMessage> reopenMQ() {
        return CQMQueues.open(basePath);
    }

}
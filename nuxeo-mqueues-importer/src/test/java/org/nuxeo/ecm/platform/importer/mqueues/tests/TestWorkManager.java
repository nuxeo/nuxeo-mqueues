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
package org.nuxeo.ecm.platform.importer.mqueues.tests;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.security.UpdateACEStatusWork;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.work.SleepWork;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.ecm.platform.importer.mqueues.workmanager.ComputationWork;
import org.nuxeo.ecm.platform.importer.mqueues.workmanager.WorkManagerComputation;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @since 9.2
 */
@Deploy({"org.nuxeo.ecm.mqueues.importer", "org.nuxeo.ecm.mqueues.importer.test"})
@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
// @LocalDeploy("org.nuxeo.ecm.mqueues.importer.test:test-workmanagercomputation-config.xml")
public class TestWorkManager {
    @Test
    public void testService() {
        WorkManagerComputation service = (WorkManagerComputation) Framework.getLocalService(WorkManager.class);
        assertNotNull(service);
    }

    @Test
    public void testWorkSerialization() {
        SleepWork work = new SleepWork(1000L, "cat", true, "some-id");
        work.setStartTime();
        work.setStatus("Foo");
        checkSerialization(work);

        UpdateACEStatusWork work2 = new UpdateACEStatusWork();
        checkSerialization(work2);
    }

    protected void checkSerialization(Work work) {
        byte[] data = ComputationWork.serialize(work);
        Work actual = ComputationWork.deserialize(data);
        assertEqualsWork(work, actual);
        assertEquals(work, ComputationWork.deserialize(ComputationWork.serialize(actual)));
    }

    protected void assertEqualsWork(Work expected, Work actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getCategory(), actual.getCategory());
        assertEquals(expected.getTitle(), actual.getTitle());
        assertEquals(expected.toString(), actual.toString());
        assertEquals(expected.getStatus(), actual.getStatus());
        assertEquals(expected, actual);
    }

    @Test
    public void testSchedule() throws InterruptedException {
        WorkManagerComputation service = (WorkManagerComputation) Framework.getLocalService(WorkManager.class);
        assertNotNull(service);
        SleepWork work = new SleepWork(1);
        service.schedule(work);
        assertTrue(service.awaitCompletion(10, TimeUnit.SECONDS));
    }

}

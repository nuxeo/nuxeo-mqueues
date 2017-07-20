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
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.security.UpdateACEStatusWork;
import org.nuxeo.ecm.core.storage.FulltextUpdaterWork;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.work.SleepWork;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.ecm.core.work.api.WorkQueueMetrics;
import org.nuxeo.ecm.platform.importer.mqueues.workmanager.ComputationWork;
import org.nuxeo.ecm.platform.importer.mqueues.workmanager.WorkManagerComputation;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @since 9.2
 */

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy({"org.nuxeo.ecm.mqueues.importer", "org.nuxeo.ecm.mqueues.importer.test"})
public abstract class TestWorkManager {

    public abstract WorkManagerComputation getService() throws Exception;

    @Inject
    protected CoreSession session;

    @Test
    public void testService() throws Exception {
        WorkManagerComputation service = getService();
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

        FulltextUpdaterWork work3 = getFulltextUpdaterWork();
        checkSerialization(work3);
    }

    protected FulltextUpdaterWork getFulltextUpdaterWork() {
        session.getRootDocument();
        List<FulltextUpdaterWork.IndexAndText> indexAndTexts = new ArrayList<>();
        indexAndTexts.add(new FulltextUpdaterWork.IndexAndText("default",
                "long string one " + new String(new char[24000]).replace('\0', 'a')));
        return new FulltextUpdaterWork(session.getRepositoryName(),
                session.getRootDocument().getId(), true,
                false, indexAndTexts);
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
        SleepWork work = new SleepWork(10);
        service.schedule(work);
        FulltextUpdaterWork work2 = getFulltextUpdaterWork();
        service.schedule(work2);
        assertTrue(service.awaitCompletion( 10, TimeUnit.SECONDS));
        assertEquals(new WorkQueueMetrics("fulltextUpdater", 0, 0, 1, 0),
                service.getMetrics("fulltextUpdater"));
        assertEquals(new WorkQueueMetrics("default", 0, 0, 1, 0),
                service.getMetrics("default"));
    }

}

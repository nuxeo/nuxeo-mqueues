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
package org.nuxeo.ecm.platform.mqueues.tests.audit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.event.impl.EventContextImpl;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.test.TransactionalFeature;
import org.nuxeo.ecm.platform.audit.AuditFeature;
import org.nuxeo.ecm.platform.audit.api.Logs;
import org.nuxeo.ecm.platform.audit.service.DefaultAuditBackend;
import org.nuxeo.ecm.platform.mqueues.MQService;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.nuxeo.ecm.platform.mqueues.audit.AuditEventListener.STREAM_CONFIG;
import static org.nuxeo.ecm.platform.mqueues.audit.AuditEventListener.STREAM_NAME;
import static org.nuxeo.ecm.platform.mqueues.audit.AuditLogWriter.COMPUTATION_NAME;

/**
 * @since 9.3
 */


@RunWith(FeaturesRunner.class)
@Features({CoreFeature.class, AuditFeature.class})
@Deploy({"org.nuxeo.ecm.platform.mqueues", "org.nuxeo.ecm.automation.core", "org.nuxeo.ecm.core.io"})
@LocalDeploy({"org.nuxeo.ecm.platform.mqueues.test:test-mq-contrib.xml",
        "org.nuxeo.ecm.platform.mqueues.test:test-audit-contrib.xml"})
public class TestAudit {

    protected static boolean waiterAdded = false;

    @Inject
    Logs serviceUnderTest;

    @Inject
    protected EventService eventService;

    @Inject
    TransactionalFeature txFeature;

    @Before
    public void addAuditWaiter() {
        if (!waiterAdded) {
            waiterAdded = true;
            txFeature.addWaiter(new AuditWaiter());
        }
    }

    public class AuditWaiter implements TransactionalFeature.Waiter {
        @Override
        public boolean await(long deadline) throws InterruptedException {
            // when there is no lag between producer and consumer we are done
            while (getMQManager().getLag(STREAM_NAME, COMPUTATION_NAME).lag() > 0) {
                if (System.currentTimeMillis() > deadline) {
                    return false;
                }
                Thread.sleep(50);
            }
            return true;
        }
    }

    public void waitForAsyncCompletion() throws InterruptedException {
        txFeature.nextTransaction(20, TimeUnit.SECONDS);
    }

    @Test
    public void testAuditLogWriter() throws Exception {
        MQManager manager = getMQManager();
        assertNotNull("stream config exists", manager);
        assertTrue("stream audit exists", manager.exists(STREAM_NAME));
    }

    private MQManager getMQManager() {
        MQService mqService = Framework.getService(MQService.class);
        return mqService.getManager(STREAM_CONFIG);
    }

    @Test
    public void testListener() throws Exception {
        waitForAsyncCompletion();
        DefaultAuditBackend backend = (DefaultAuditBackend) serviceUnderTest;
        List<String> eventIds = backend.getLoggedEventIds();

        // eventIds.forEach(e -> System.out.println("before event " + e));
        // System.out.println("before: " + eventIds.size());

        int n = eventIds.size();

        EventContext ctx = new EventContextImpl();
        Event event = ctx.newEvent("documentModified");
        event.setInline(false);
        event.setImmediate(true);
        eventService.fireEvent(event);
        waitForAsyncCompletion();

        eventIds = backend.getLoggedEventIds();
        // eventIds.forEach(e -> System.out.println("logged event " + e));
        // System.out.println("total: " + eventIds.size());
        assertEquals(n + 1, eventIds.size());
    }


}

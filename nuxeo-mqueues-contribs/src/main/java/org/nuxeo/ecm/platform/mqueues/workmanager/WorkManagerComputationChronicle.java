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

package org.nuxeo.ecm.platform.mqueues.workmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.mqueues.chronicle.ChronicleConfig;
import org.nuxeo.lib.core.mqueues.computation.Record;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.chronicle.ChronicleMQManager;

import java.nio.file.Path;


/**
 * @since 9.2
 */
public class WorkManagerComputationChronicle extends WorkManagerComputation {
    protected static final Log log = LogFactory.getLog(WorkManagerComputationChronicle.class);

    @Override
    protected MQManager<Record> initStream() {
        Path basePath = ChronicleConfig.getBasePath("work");
        String retentionDuration = ChronicleConfig.getRetentionDuration();
        log.info("Init WorkManagerComputation using Chronicle MQueue impl, basePath: " + basePath
                + " and retention duration: " + retentionDuration);
        return new ChronicleMQManager<>(basePath, retentionDuration);
    }

    @Override
    protected int getOverProvisioningFactor() {
        // Chronicle is for single node mode so we don't over provision partitions
        return 1;
    }
}

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
package org.nuxeo.ecm.platform.importer.mqueues.chronicle;

import org.nuxeo.common.Environment;
import org.nuxeo.runtime.api.Framework;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @since 9.2
 */
public class ChronicleConfig {
    public static final String NUXEO_MQUEUE_DIR_PROP = "nuxeo.mqueue.chronicle.dir";

    public static final String NUXEO_MQUEUE_RET_DURATION_PROP = "nuxeo.mqueue.chronicle.retention.duration";

    /**
     * Returns the base path to store Chronicle Queues
     */
    public static Path getBasePath(String name) {
        String ret = Framework.getProperty(NUXEO_MQUEUE_DIR_PROP);
        if (ret != null) {
            return Paths.get(ret).toAbsolutePath();
        }
        ret = Framework.getProperty(Environment.NUXEO_DATA_DIR);
        if (ret != null) {
            return Paths.get(ret, "mqueue", name).toAbsolutePath();
        }
        return Paths.get(Framework.getRuntime().getHome().getAbsolutePath(),
                "data", "mqueue", name).toAbsolutePath();
    }

    /**
     * Returns the retention duration property
     */
    public static String getRetentionDuration() {
        return Framework.getProperty(NUXEO_MQUEUE_RET_DURATION_PROP);
    }

}

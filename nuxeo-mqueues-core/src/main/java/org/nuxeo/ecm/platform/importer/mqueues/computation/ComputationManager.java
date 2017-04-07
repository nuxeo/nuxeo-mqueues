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
package org.nuxeo.ecm.platform.importer.mqueues.computation;

import java.time.Duration;

/**
 * @since 9.2
 */
public interface ComputationManager {
    /**
     * Run the computations
     */
    void start();

    /**
     * Stop computations gracefully after processing a record or a timer.
     */
    boolean stop(Duration timeout);

    boolean stop();

    /**
     * Stop computations when input streams are empty.
     * The timeout is applied for each computation, the total duration can be up to nb computations * timeout
     * Returns true if computations are stopped during the timeout delay.
     */
    boolean drainAndStop(Duration timeout);

    /**
     * Shutdown immediately
     */
    void shutdown();

    long getLowWatermark();

    long getLowWatermark(String computationName);

    boolean isDone(long timestamp);
}

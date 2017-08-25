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
 * Run a topology of computations according to some settings.
 *
 * @since 9.2
 */
public interface ComputationManager {

    /**
     * Run the computations defined by the topology and settings.
     */
    void start(Topology topology, Settings settings);

    /**
     * Stop computations gracefully after processing a record or a timer.
     */
    boolean stop(Duration timeout);

    /**
     * Stop computations when input streams are empty.
     * The timeout is applied for each computation, the total duration can be up to nb computations * timeout
     * <p/>
     * Returns {@code true} if computations are stopped during the timeout delay.
     */
    boolean drainAndStop(Duration timeout);

    /**
     * Shutdown immediately.
     */
    void shutdown();

    /**
     * Return the low watermark for the computation.
     * Any message with an offset below the low watermark has been processed by this computation and its ancestors..
     */
    long getLowWatermark(String computationName);

    /**
     * Return the low watermark for all the computations of the topology.
     * Any message with an offset below the low watermark has been processed.
     */
    long getLowWatermark();

    /**
     * Returns true if all messages with a lower timestamp has been processed by the topology.
     */
    boolean isDone(long timestamp);


    /**
     * Wait for the computations to have assigned partitions ready to process records.
     * This is useful for writing unit test.
     * <p/>
     * Returns {@code true} if all computations have assigned partitions during the timeout delay.
     */
    boolean waitForAssignments(Duration timeout) throws InterruptedException;

}

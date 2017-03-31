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
package org.nuxeo.ecm.platform.importer.mqueues.tests.computation;

import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationContext;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;

/**
 *
 * @since 9.1
 */
public class ComputationForwardSlow extends ComputationForward {


    private final int averageDelayMs;

    public ComputationForwardSlow(String name, int inputs, int outputs, int averageDelayMs) {
        super(name, inputs, outputs);
        this.averageDelayMs = averageDelayMs;
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
        try {
            Thread.sleep(averageDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Interrupted");
        }
        super.processRecord(context, inputStreamName, record);
    }
}

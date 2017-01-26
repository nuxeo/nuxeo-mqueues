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
package org.nuxeo.ecm.platform.importer.mqueues.consumer;

/**
 * Keep state of a batch according to a batch policy.
 *
 * @since 9.1
 */
public class BatchState {
    private final BatchPolicy policy;
    private int counter;
    private long endMs;

    public BatchState(BatchPolicy policy) {
        this.policy = policy;
    }

    public void start() {
        endMs = System.currentTimeMillis() + policy.getThresholdMs();
        counter = 0;
    }

    public void inc() {
        counter++;
    }

    public boolean isFull() {
        if (counter >= policy.getCapacity()) {
            return true;
        }
        return System.currentTimeMillis() > endMs;
    }

    public int getSize() {
        return counter;
    }

}

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
 * The return status of a {@link ConsumerCallable}
 *
 * @since 9.1
 */
public class ConsumerStatus {
    public final int consumer;
    public final long startTime;
    public final long stopTime;
    public final long accepted;
    public final long committed;
    public final long batchFailure;
    public final long batchCommit;

    public ConsumerStatus(int consumer, long accepted, long committed, long batchCommit, long batchFailure, long startTime, long stop) {
        this.consumer = consumer;
        this.accepted = accepted;
        this.committed = committed;
        this.batchCommit = batchCommit;
        this.batchFailure = batchFailure;
        this.startTime = startTime;
        this.stopTime = stop;
    }
}

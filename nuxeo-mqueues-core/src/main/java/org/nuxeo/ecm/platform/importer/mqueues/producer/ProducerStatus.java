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
package org.nuxeo.ecm.platform.importer.mqueues.producer;

/**
 * The return status of a {@link ProducerLoop}
 *
 * @since 9.1
 */
public class ProducerStatus {
    public final long startTime;
    public final long stopTime;
    public final long nbProcessed;
    public final int producer;

    public ProducerStatus(int producer, long nbProcessed, long startTime, long stopTime) {
        this.producer = producer;
        this.nbProcessed = nbProcessed;
        this.startTime = startTime;
        this.stopTime = stopTime;
    }
}

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

package org.nuxeo.ecm.platform.mqueues.importer.producer;

import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.ecm.platform.mqueues.importer.producer.BlobInfoFetcher;
import org.nuxeo.ecm.platform.mqueues.importer.message.BlobInfoMessage;
import org.nuxeo.ecm.platform.mqueues.importer.message.DocumentMessage;
import org.nuxeo.lib.core.mqueues.mqueues.MQRecord;
import org.nuxeo.lib.core.mqueues.mqueues.MQTailer;

import java.time.Duration;


/**
 * Returns blob information from a MQueue, loop on the queue.
 *
 * @since 9.3
 */
public class RandomMQBlobInfoFetcher implements BlobInfoFetcher {
    protected static final int READ_DELAY_MS = 10;
    protected final MQTailer<BlobInfoMessage> tailer;

    public RandomMQBlobInfoFetcher(MQTailer<BlobInfoMessage> blobInfoTailer) {
        this.tailer = blobInfoTailer;
    }

    @Override
    public BlobInfo get(DocumentMessage.Builder builder) {
        MQRecord<BlobInfoMessage> record;
        try {
            record = tailer.read(Duration.ofMillis(READ_DELAY_MS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (record == null) {
            // start again from beginning
            tailer.toStart();
            return get(builder);
        }
        return record.message();
    }

    @Override
    public void close() throws Exception {
        tailer.close();
    }
}

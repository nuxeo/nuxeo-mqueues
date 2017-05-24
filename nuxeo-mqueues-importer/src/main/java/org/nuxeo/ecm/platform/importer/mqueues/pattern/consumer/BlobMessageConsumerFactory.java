/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer;

import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobMessage;

import java.nio.file.Path;

/**
 * @since 9.1
 */
public class BlobMessageConsumerFactory implements ConsumerFactory<BlobMessage> {
    private final String blobProviderName;
    private final Path outputBlobInfoDirectory;

    /**
     * Blob Consumer factory requires a blob provderName that is present in Nuxeo instance running the consumer.
     * The outputBlobInofDirectory is used as a base path to store the blob informations in csv files.
     *
     */
    public BlobMessageConsumerFactory(String blobProviderName, Path outputBlobInfoDirectory) {
        this.blobProviderName = blobProviderName;
        this.outputBlobInfoDirectory = outputBlobInfoDirectory;
    }

    @Override
    public Consumer<BlobMessage> createConsumer(int consumerId) {
        return new BlobMessageConsumer(consumerId, blobProviderName, outputBlobInfoDirectory);
    }
}

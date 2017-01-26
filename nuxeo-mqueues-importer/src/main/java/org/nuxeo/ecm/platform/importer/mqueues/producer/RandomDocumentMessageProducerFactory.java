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

import org.nuxeo.ecm.platform.importer.mqueues.message.DocumentMessage;

import java.nio.file.Path;

/**
 * @since 9.1
 */
public class RandomDocumentMessageProducerFactory implements ProducerFactory<DocumentMessage> {
    private final long nbDocuments;
    private final String lang;
    private final Path blobInfoDirectory;
    private final int blobSizeKb;

    /**
     * Generates random document messages that contains random blob.
     */
    public RandomDocumentMessageProducerFactory(long nbDocuments, String lang, int blobSizeKb) {
        this.nbDocuments = nbDocuments;
        this.lang = lang;
        this.blobInfoDirectory = null;
        this.blobSizeKb = blobSizeKb;
    }

    /**
     * Generates random documents messages that point to existing blobs.
     */
    public RandomDocumentMessageProducerFactory(long nbDocuments, String lang, Path blobInfoDirectory) {
        this.nbDocuments = nbDocuments;
        this.lang = lang;
        this.blobInfoDirectory = blobInfoDirectory;
        this.blobSizeKb = 0;
    }

    @Override
    public Producer<DocumentMessage> createProducer(int producerId) {
        return new RandomDocumentMessageProducer(producerId, nbDocuments, lang, blobInfoDirectory).withBlob(blobSizeKb, false);
    }
}

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

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.impl.blob.FileBlob;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.BlobProvider;
import org.nuxeo.ecm.platform.importer.mqueues.message.BlobMessage;
import org.nuxeo.runtime.api.Framework;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @since 9.1
 */
public class BlobMessageConsumer extends AbstractConsumer<BlobMessage> {
    private static final java.lang.String DEFAULT_ENCODING = "UTF-8";
    private static final String HEADER = "key, digest, length, filename, mimetype, encoding\n";
    private final BlobProvider blobProvider;
    private final PrintWriter outputWriter;
    private final FileWriter outputFileWriter;
    private final String blobProviderName;

    public BlobMessageConsumer(int consumerId, String blobProviderName, Path outputBlobInfoDirectory) {
        super(consumerId);
        this.blobProviderName = blobProviderName;
        this.blobProvider = Framework.getService(BlobManager.class).getBlobProvider(blobProviderName);
        if (blobProvider == null) {
            throw new IllegalArgumentException("Invalid blob provider: " + blobProviderName);
        }
        Path outputFile = Paths.get(outputBlobInfoDirectory.toString(), "bi-" + consumerId + ".csv");
        try {
            outputBlobInfoDirectory.toFile().mkdirs();
            outputFileWriter = new FileWriter(outputFile.toFile(), true);
            BufferedWriter bw = new BufferedWriter(outputFileWriter);
            outputWriter = new PrintWriter(bw);
            outputWriter.write(HEADER);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid output path: " + outputFile, e);
        }
    }

    @Override
    public void close() throws Exception {
        outputWriter.close();
        outputFileWriter.close();
    }

    @Override
    public void begin() {

    }

    @Override
    public void accept(BlobMessage message) {
        try {
            Blob blob;
            if (message.getPath() != null) {
                blob = new FileBlob(new File(message.getPath()));
            } else {
                // we don't submit filename or encoding this is not saved in the binary store but in the document
                blob = new StringBlob(message.getContent(), null, null, null);
            }
            BlobInfo bi = new BlobInfo();
            bi.digest = blobProvider.writeBlob(blob);
            bi.key = blobProviderName + ":" + bi.digest;
            bi.length = blob.getLength();
            bi.filename = message.getFilename();
            bi.mimeType = message.getMimetype();
            bi.encoding = message.getEncoding();
            saveBlobInfo(bi);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid blob: " + message, e);
        }
    }

    private void saveBlobInfo(BlobInfo bi) {
        outputWriter.write(String.format("%s, %s, %d, \"%s\", %s, %s\n",
                bi.key, bi.digest, bi.length, sanitize(bi.filename), sanitize(bi.mimeType), sanitize(bi.encoding)));
    }

    private String sanitize(String str) {
        if (str == null || str.trim().isEmpty()) {
            return "";
        }
        return str;
    }

    @Override
    public void commit() {
        outputWriter.flush();
    }

    @Override
    public void rollback() {

    }
}

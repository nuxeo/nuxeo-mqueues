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

package org.nuxeo.ecm.platform.importer.mqueues.pattern.internals;

import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.lib.core.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.BlobInfoWriter;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.BlobInfoMessage;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.message.DocumentMessage;


/**
 * Write blob information to a MQueue
 *
 * @since 9.3
 */
public class MQBlobInfoWriter implements BlobInfoWriter {
    protected final MQAppender<BlobInfoMessage> appender;

    public MQBlobInfoWriter(MQAppender<BlobInfoMessage> blobInfoAppender) {
        this.appender = blobInfoAppender;
    }

    @Override
    public void save(DocumentMessage.Builder builder, BlobInfo info) {
        appender.append(info.digest, new BlobInfoMessage(info));
    }

    @Override
    public void close() throws Exception {
        appender.close();
    }

}

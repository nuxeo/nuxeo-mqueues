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
package org.nuxeo.ecm.platform.mqueues.importer.consumer;

import org.nuxeo.ecm.platform.mqueues.importer.message.DocumentMessage;
import org.nuxeo.lib.core.mqueues.pattern.consumer.Consumer;
import org.nuxeo.lib.core.mqueues.pattern.consumer.ConsumerFactory;

/**
 * @since 9.1
 */
public class DocumentMessageConsumerFactory implements ConsumerFactory<DocumentMessage> {
    protected final String repositoryName;
    protected final String rootPath;

    public DocumentMessageConsumerFactory(String repositoryName, String rootPath) {
        this.repositoryName = repositoryName;
        this.rootPath = rootPath;
    }

    @Override
    public Consumer<DocumentMessage> createConsumer(String consumerId) {
        return new DocumentMessageConsumer(consumerId, repositoryName, rootPath);
    }
}

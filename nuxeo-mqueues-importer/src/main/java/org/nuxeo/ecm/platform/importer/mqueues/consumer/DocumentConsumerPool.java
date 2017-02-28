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

import org.nuxeo.ecm.core.event.EventServiceAdmin;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.runtime.api.Framework;

/**
 * Block Nuxeo listeners during import.
 *
 * @since 9.1
 */
public class DocumentConsumerPool<M extends Message> extends ConsumerPool<M> {

    protected static final String NOTIF_LISTENER = "notificationListener";

    protected static final String MIME_LISTENER = "mimetypeIconUpdater";

    protected static final String INDEXING_LISTENER = "elasticSearchInlineListener";

    public DocumentConsumerPool(MQueues<M> qm, ConsumerFactory<M> factory, ConsumerPolicy policy) {
        super(qm, factory, policy);
        EventServiceAdmin eventAdmin = Framework.getLocalService(EventServiceAdmin.class);
        if (eventAdmin == null) {
            return;
        }
        // TODO: make this configurable
        eventAdmin.setBulkModeEnabled(true);
        eventAdmin.setBlockAsyncHandlers(true);
        eventAdmin.setBlockSyncPostCommitHandlers(true);
        eventAdmin.setListenerEnabledFlag(MIME_LISTENER, false);
        eventAdmin.setListenerEnabledFlag(NOTIF_LISTENER, false);
        eventAdmin.setListenerEnabledFlag(INDEXING_LISTENER, false);
    }


    @Override
    public void close() throws Exception {
        super.close();

        EventServiceAdmin eventAdmin = Framework.getLocalService(EventServiceAdmin.class);
        if (eventAdmin != null) {
            eventAdmin.setBulkModeEnabled(false);
            eventAdmin.setBlockAsyncHandlers(false);
            eventAdmin.setBlockSyncPostCommitHandlers(false);
            eventAdmin.setListenerEnabledFlag(NOTIF_LISTENER, true);
            eventAdmin.setListenerEnabledFlag(MIME_LISTENER, true);
            eventAdmin.setListenerEnabledFlag(INDEXING_LISTENER, true);
        }

    }


}

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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.event.EventServiceAdmin;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.Message;
import org.nuxeo.runtime.api.Framework;

/**
 * Block Nuxeo listeners during import.
 *
 * @since 9.1
 */
public class DocumentConsumerPool<M extends Message> extends ConsumerPool<M> {
    private static final Log log = LogFactory.getLog(DocumentConsumerPool.class);
    protected final DocumentConsumerPolicy policy;
    protected static final String NOTIF_LISTENER = "notificationListener";
    protected static final String MIME_LISTENER = "mimetypeIconUpdater";
    protected static final String INDEXING_LISTENER = "elasticSearchInlineListener";
    protected static final String DUBLICORE_LISTENER = "dclistener";
    protected static final String TPL_LISTENER = "templateCreator";
    protected static final String BINARY_LISTENER = "binaryMetadataSyncListener";
    protected static final String UID_LISTENER = "uidlistener";

    public DocumentConsumerPool(String mqName, MQManager<M> manager, ConsumerFactory<M> factory, ConsumerPolicy consumerPolicy) {
        super(mqName, manager, factory, consumerPolicy);
        EventServiceAdmin eventAdmin = Framework.getLocalService(EventServiceAdmin.class);
        policy = (DocumentConsumerPolicy) consumerPolicy;
        if (eventAdmin == null) {
            return;
        }
        if (policy.blockIndexing()) {
            eventAdmin.setListenerEnabledFlag(INDEXING_LISTENER, false);
            log.info("Block ES indexing");
        }
        if (policy.blockAsyncListeners()) {
            eventAdmin.setBlockAsyncHandlers(true);
            log.info("Block asynchronous listeners");
        }
        if (policy.blockPostCommitListeners()) {
            eventAdmin.setBlockSyncPostCommitHandlers(true);
            log.info("Block post commit listeners");
        }
        if (policy.blockDefaultSyncListeners()) {
            eventAdmin.setListenerEnabledFlag(NOTIF_LISTENER, false);
            eventAdmin.setListenerEnabledFlag(MIME_LISTENER, false);
            eventAdmin.setListenerEnabledFlag(DUBLICORE_LISTENER, false);
            eventAdmin.setListenerEnabledFlag(TPL_LISTENER, false);
            eventAdmin.setListenerEnabledFlag(BINARY_LISTENER, false);
            eventAdmin.setListenerEnabledFlag(UID_LISTENER, false);
            log.info("Block some default synchronous listener");
        }
        if (policy.bulkMode()) {
            eventAdmin.setBulkModeEnabled(true);
            log.info("Enable bulk mode");
        }
    }


    @Override
    public void close() throws Exception {
        super.close();

        EventServiceAdmin eventAdmin = Framework.getLocalService(EventServiceAdmin.class);
        if (eventAdmin == null) {
            return;
        }
        if (policy.blockIndexing()) {
            eventAdmin.setListenerEnabledFlag(INDEXING_LISTENER, true);
            log.info("Unblock ES indexing");
        }
        if (policy.blockAsyncListeners()) {
            eventAdmin.setBlockAsyncHandlers(false);
            log.info("Unblock asynchronous listeners");
        }
        if (policy.blockPostCommitListeners()) {
            eventAdmin.setBlockSyncPostCommitHandlers(false);
            log.info("Unblock post commit listeners");
        }
        if (policy.blockDefaultSyncListeners()) {
            eventAdmin.setListenerEnabledFlag(NOTIF_LISTENER, true);
            eventAdmin.setListenerEnabledFlag(MIME_LISTENER, true);
            eventAdmin.setListenerEnabledFlag(DUBLICORE_LISTENER, true);
            eventAdmin.setListenerEnabledFlag(TPL_LISTENER, true);
            eventAdmin.setListenerEnabledFlag(BINARY_LISTENER, true);
            eventAdmin.setListenerEnabledFlag(UID_LISTENER, true);
            log.info("Unblock some default synchronous listener");
        }
        if (policy.bulkMode()) {
            eventAdmin.setBulkModeEnabled(false);
            log.info("Disable bulk mode");
        }
    }


}

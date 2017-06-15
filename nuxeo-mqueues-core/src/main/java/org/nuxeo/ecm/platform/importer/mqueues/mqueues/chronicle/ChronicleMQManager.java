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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRebalanceListener;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.AbstractMQManager;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

import static org.apache.commons.io.FileUtils.deleteDirectory;

/**
 * @since 9.2
 */
public class ChronicleMQManager<M extends Externalizable> extends AbstractMQManager<M> {
    private static final Log log = LogFactory.getLog(ChronicleMQManager.class);
    private final Path basePath;

    public ChronicleMQManager(Path basePath) {
        this.basePath = basePath;
    }

    public String getBasePath() {
        return basePath.toAbsolutePath().toString();
    }

    @Override
    public boolean exists(String name) {
        File path = new File(basePath.toFile(), name);
        return path.isDirectory() && path.list().length > 0;
    }

    @Override
    public void create(String name, int size) {
        ChronicleMQAppender<M> cq = ChronicleMQAppender.create(new File(basePath.toFile(), name), size);
        try {
            cq.close();
        } catch (Exception e) {
            throw new RuntimeException("Can not create and close " + name, e);
        }
    }

    @Override
    public boolean delete(String name) {
        File path = new File(basePath.toFile(), name);
        if (path.isDirectory()) {
            deleteQueueBasePath(path);
            return true;
        }
        return false;
    }


    @Override
    public MQAppender<M> createAppender(String name) {
        return ChronicleMQAppender.open(new File(basePath.toFile(), name));
    }

    @Override
    protected MQTailer<M> acquireTailer(Collection<MQPartition> partitions, String group) {
        Collection<ChronicleMQTailer<M>> pTailers = new ArrayList<>(partitions.size());
        partitions.forEach(partition -> pTailers.add((ChronicleMQTailer<M>) ((ChronicleMQAppender<M>) getAppender(partition.name())).createTailer(partition, group)));
        if (pTailers.size() == 1) {
            return pTailers.iterator().next();
        }
        return new ChronicleCompoundMQTailer<>(pTailers, group);
    }

    @Override
    protected MQTailer<M> doSubscribe(String group, Collection<String> names, MQRebalanceListener listener) {
        throw new UnsupportedOperationException("subscribe is not supported by Chronicle implementation");

    }

    private static void deleteQueueBasePath(File basePath) {
        try {
            log.info("Removing Chronicle Queues directory: " + basePath);
            // Performs a recursive delete if the directory contains only chronicles files
            try (Stream<Path> paths = Files.list(basePath.toPath())) {
                int count = (int) paths.filter(path -> (Files.isRegularFile(path) && !path.toString().endsWith(".cq4"))).count();
                if (count > 0) {
                    String msg = "ChronicleMQueue basePath: " + basePath + " contains unknown files, please choose another basePath";
                    log.error(msg);
                    throw new IllegalArgumentException(msg);
                }
            }
            deleteDirectory(basePath);
        } catch (IOException e) {
            String msg = "Can not remove Chronicle Queues directory: " + basePath + " " + e.getMessage();
            log.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

}

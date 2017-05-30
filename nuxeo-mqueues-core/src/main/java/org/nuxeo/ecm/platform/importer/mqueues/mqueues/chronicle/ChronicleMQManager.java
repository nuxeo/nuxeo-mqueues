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
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.apache.commons.io.FileUtils.deleteDirectory;

/**
 * @since 9.2
 */
public class ChronicleMQManager<M extends Externalizable> extends MQManager<M> {
    private static final Log log = LogFactory.getLog(ChronicleMQManager.class);
    private final Path basePath;

    public ChronicleMQManager(Path basePath) {
        this.basePath = basePath;
    }

    @Override
    public boolean exists(String name) {
        File path = new File(basePath.toFile(), name);
        return path.isDirectory() && path.list().length > 0;
    }

    @Override
    public MQueue<M> open(String name) {
        return ChronicleMQueue.open(new File(basePath.toFile(), name));
    }

    @Override
    public MQueue<M> create(String name, int size) {
        return ChronicleMQueue.create(new File(basePath.toFile(), name), size);
    }

    static public boolean delete(File basePath) {
        if (basePath.isDirectory()) {
            deleteQueueBasePath(basePath);
            return true;
        }
        return false;
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

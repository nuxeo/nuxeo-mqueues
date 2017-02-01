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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.nuxeo.ecm.platform.importer.mqueues.mqueues.CQTailer.DEFAULT_OFFSET_NAMESPACE;

/**
 * Chronicle Queue implementation of MQueues.
 *
 * Note that for performance reason the class loader assertion are disabled.
 *
 * @since 9.1
 */
public class CQMQueues<M extends Message> implements MQueues<M> {
    private static final Log log = LogFactory.getLog(CQMQueues.class);
    private static final String QUEUE_PREFIX = "Q-";
    private static final int POLL_INTERVAL_MS = 100;

    private final List<ChronicleQueue> queues;
    private final int nbQueues;
    private final File basePath;

    /**
     * Create a new mqueues. Warning this will ERASE the basePath if not empty.
     */
    public CQMQueues(File basePath, int size) {
        this(basePath, size, false);
    }

    /**
     * Open an existing mqueues.
     */
    public CQMQueues(File basePath) {
        this(basePath, 0, true);
    }


    @Override
    public int size() {
        return nbQueues;
    }

    @Override
    public CQOffset append(int queue, M message) {
        ExcerptAppender appender = queues.get(queue).acquireAppender();
        appender.writeDocument(w -> w.write("msg").object(message));
        return new CQOffset(queue, appender.lastIndexAppended());
    }

    @Override
    public Tailer<M> createTailer(int queue) {
        return new CQTailer<>(basePath.toString(), queues.get(queue).createTailer(), queue);
    }

    @Override
    public Tailer<M> createTailer(int queue, String name) {
        return new CQTailer<>(basePath.toString(), queues.get(queue).createTailer(), queue, name);
    }

    @Override
    public boolean waitFor(Offset offset, Duration timeout) throws InterruptedException {
        boolean ret;
        long offsetPosition = ((CQOffset) offset).getOffset();
        int queue = ((CQOffset) offset).getQueue();
        try (CQOffsetTracker offsetTracker = new CQOffsetTracker(basePath.toString(), queue, DEFAULT_OFFSET_NAMESPACE)) {
            ret = isProcessed(offsetTracker, offsetPosition);
            if (ret) {
                return true;
            }
            final long timeoutMs = timeout.toMillis();
            final long deadline = System.currentTimeMillis() + timeoutMs;
            final long delay = Math.min(POLL_INTERVAL_MS, timeoutMs);
            while (!ret && System.currentTimeMillis() < deadline) {
                Thread.sleep(delay);
                ret = isProcessed(offsetTracker, offsetPosition);
            }
        }
        return ret;
    }

    private boolean isProcessed(CQOffsetTracker tracker, long offset) {
        long last = tracker.readLastCommittedOffset();
        return (last > 0) && (last >= offset);
    }


    @Override
    public void close() throws Exception {
        log.debug("Closing queue");
        queues.stream().filter(Objects::nonNull).forEach(ChronicleQueue::close);
        queues.clear();
    }

    private CQMQueues(File basePath, int size, boolean append) {
        if (!append) {
            resetBasePath(basePath);
            this.nbQueues = size;
        } else {
            this.nbQueues = findNbQueues(basePath);
        }
        this.basePath = basePath;
        queues = new ArrayList<>(this.nbQueues);
        log.info("Using chronicle queues in: " + basePath);

        for (int i = 0; i < this.nbQueues; i++) {
            File path = new File(basePath, String.format("%s%02d", QUEUE_PREFIX, i));
            ChronicleQueue queue = binary(path).build();
            queues.add(queue);
            // touch the queue so we can count them even if they stay empty.
            queue.file().mkdirs();
        }

        // When manipulating millions of messages java assert must be disable or GC on Chronicle Queues will knock at the door
        // also this does not work when running test suite, it requires to change the maven-surefire-plugin conf to add a -da option
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        loader.setDefaultAssertionStatus(false);
    }

    private int findNbQueues(File basePath) {
        int ret;
        try (Stream<Path> paths = Files.list(basePath.toPath())) {
            ret = (int) paths.filter(path -> (Files.isDirectory(path) && path.getFileName().toString().startsWith(QUEUE_PREFIX))).count();
            if (ret == 0) {
                throw new IOException("No chronicles queues file found");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid basePath for queue: " + basePath, e);
        }
        return ret;
    }

    private void resetBasePath(File basePath) {
        if (basePath.isDirectory()) {
            deleteQueueBasePath(basePath);
        }
        if (!basePath.mkdirs()) {
            String msg = "Can not create Chronicle Queues in: " + basePath;
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private void deleteQueueBasePath(File basePath) {
        try {
            log.info("Removing Chronicle Queues directory: " + basePath);
            // Performs a recursive delete if the directory contains only chronicles files
            try (Stream<Path> paths = Files.list(basePath.toPath())) {
                int count = (int) paths.filter(path -> (Files.isRegularFile(path) && !path.toString().endsWith(".cq4"))).count();
                if (count > 0) {
                    String msg = "CQMQueues basePath: " + basePath + " contains unkown files, please choose another basePath";
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

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
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.apache.commons.io.FileUtils.deleteDirectory;

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
    private static final String OFFSET_QUEUE_PREFIX = "offset-";
    private static final long POLL_INTERVAL_MS = 100L;

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
    public void append(int queue, M message) {
        queues.get(queue).acquireAppender().writeDocument(w -> w.write("msg").object(message));
    }

    @Override
    public Tailer<M> createTailer(int queue) {
        return new CQTailer(queue);
    }

    @Override
    public Tailer<M> createTailer(int queue, String name) {
        return new CQTailer(queue, name);
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


    public class CQTailer implements Tailer<M> {
        private final int queueIndex;
        private final ExcerptTailer tailer;
        private final File offsetFile;
        private final SingleChronicleQueue offsetQueue;
        private long lastCommittedOffset = 0;

        public CQTailer(int queue) {
            // Use the same offset queue for all tailers
            this(queue, "all");
        }

        public CQTailer(int queue, String offsetName) {
            queueIndex = queue;
            tailer = queues.get(queueIndex).createTailer();
            offsetFile = new File(basePath, OFFSET_QUEUE_PREFIX + offsetName);
            offsetQueue = binary(getOffsetFile()).build();
            toLastCommitted();
        }

        @Override
        public M read(long timeout, TimeUnit unit) throws InterruptedException {
            M ret = read();
            if (ret != null) {
                return ret;
            }
            final long timeoutMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
            final long deadline = System.currentTimeMillis() + timeoutMs;
            final long delay = Math.min(POLL_INTERVAL_MS, timeoutMs);
            while (ret == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(delay);
                ret = read();
            }
            return ret;
        }

        @SuppressWarnings("unchecked")
        private M read() {
            final List<M> ret = new ArrayList<>(1);
            if (tailer.readDocument(w -> ret.add((M) w.read("msg").object()))) {
                return ret.get(0);
            }
            return null;
        }

        @Override
        public void commit() {
            // we write raw: queue, offset, timestamp
            long offset = tailer.index();
            offsetQueue.acquireAppender().writeBytes(b -> b.writeInt(queueIndex).writeLong(offset).writeLong(System.currentTimeMillis()));
            if (log.isTraceEnabled()) {
                log.trace(String.format("queue-%02d commit offset: %d", queueIndex, offset));
            }
            lastCommittedOffset = offset;
        }

        @Override
        public void toEnd() {
            log.debug(String.format("queue-%02d toEnd", queueIndex));
            tailer.toEnd();
        }

        @Override
        public void toStart() {
            log.debug(String.format("queue-%02d toStart", queueIndex));
            tailer.toStart();
        }

        @Override
        public void toLastCommitted() {
            if (lastCommittedOffset > 0) {
                log.debug(String.format("queue-%02d toLastCommitted: %d", queueIndex, lastCommittedOffset));
                tailer.moveToIndex(lastCommittedOffset);
                return;
            }
            ExcerptTailer offsetTailer = offsetQueue.createTailer().direction(TailerDirection.BACKWARD).toEnd();
            final boolean[] found = {false};
            boolean hasNext;
            do {
                hasNext = offsetTailer.readBytes(b -> {
                    int queue = b.readInt();
                    long offset = b.readLong();
                    long stamp = b.readLong();
                    if (queueIndex == queue) {
                        log.debug(String.format("queue-%02d toLastCommitted found: %d", queueIndex, offset));
                        tailer.moveToIndex(offset);
                        found[0] = true;
                    }
                });
            } while (!found[0] && hasNext);
            if (!found[0]) {
                log.debug(String.format("queue-%02d toLastCommitted not found, start from beginning", queueIndex));
                tailer.toStart();
            }
        }

        @Override
        public int getQueue() {
            return queueIndex;
        }

        private File getOffsetFile() {
            return offsetFile;
        }

        @Override
        public void close() throws Exception {
            offsetQueue.close();
        }
    }


}

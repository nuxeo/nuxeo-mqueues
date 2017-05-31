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

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQOffsetImpl;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/**
 * Chronicle Queue implementation of MQueue.
 *
 * Note that for performance reason the class loader assertion are disabled.
 *
 * @since 9.1
 */
public class ChronicleMQueue<M extends Externalizable> implements MQueue<M> {
    private static final Log log = LogFactory.getLog(ChronicleMQueue.class);
    private static final String QUEUE_PREFIX = "Q-";
    private static final int POLL_INTERVAL_MS = 100;

    private final List<ChronicleQueue> queues;
    private final int nbQueues;
    private final File basePath;
    private final String name;

    // keep track of created tailers to make sure they are closed before the mq
    private final ConcurrentLinkedQueue<ChronicleMQTailer<M>> tailers = new ConcurrentLinkedQueue<>();

    static public boolean exists(File basePath) {
        return basePath.isDirectory() && basePath.list().length > 0;
    }

    public String getBasePath() {
        return basePath.getPath();
    }

    /**
     * Create a new mqueues.
     */
    static public <M extends Externalizable> ChronicleMQueue<M> create(File basePath, int size) {
        return new ChronicleMQueue<>(basePath, size);
    }

    /**
     * Open an existing mqueues.
     */
    static public <M extends Externalizable> ChronicleMQueue<M> open(File basePath) {
        return new ChronicleMQueue<>(basePath, 0);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int size() {
        return nbQueues;
    }

    @Override
    public MQOffsetImpl append(int queue, M message) {
        ExcerptAppender appender = queues.get(queue).acquireAppender();
        appender.writeDocument(w -> w.write("msg").object(message));
        return new MQOffsetImpl(queue, appender.lastIndexAppended());
    }

    @Override
    public MQTailer<M> createTailer(int queue, String nameSpace) {
        return addTailer(new ChronicleMQTailer<>(basePath.toString(), queues.get(queue).createTailer(), queue, nameSpace));
    }

    private MQTailer<M> addTailer(ChronicleMQTailer<M> tailer) {
        tailers.add(tailer);
        return tailer;
    }

    @Override
    public boolean waitFor(MQOffset offset, String nameSpace, Duration timeout) throws InterruptedException {
        boolean ret;
        long offsetPosition = ((MQOffsetImpl) offset).getOffset();
        int queue = ((MQOffsetImpl) offset).getQueue();
        try (ChronicleMQOffsetTracker offsetTracker = new ChronicleMQOffsetTracker(basePath.toString(), queue, nameSpace)) {
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

    private boolean isProcessed(ChronicleMQOffsetTracker tracker, long offset) {
        long last = tracker.readLastCommittedOffset();
        return (last > 0) && (last >= offset);
    }


    @Override
    public void close() throws Exception {
        log.debug("Closing queue");
        tailers.stream().filter(Objects::nonNull).forEach(tailer -> {
            try {
                tailer.close();
            } catch (Exception e) {
                log.error("Failed to close tailer: " + tailer);
            }
        });
        tailers.clear();
        queues.stream().filter(Objects::nonNull).forEach(ChronicleQueue::close);
        queues.clear();
    }

    private ChronicleMQueue(File basePath, int size) {
        if (size == 0) {
            // open
            if (!exists(basePath)) {
                String msg = "Can not open Chronicle Queues, invalid path: " + basePath;
                log.error(msg);
                throw new IllegalArgumentException(msg);
            }
            this.nbQueues = findNbQueues(basePath);
        } else {
            // creation
            if (exists(basePath)) {
                String msg = "Can not create Chronicle Queues, already exists: " + basePath;
                log.error(msg);
                throw new IllegalArgumentException(msg);
            }
            if (!basePath.exists() && !basePath.mkdirs()) {
                String msg = "Can not create Chronicle Queues in: " + basePath;
                log.error(msg);
                throw new IllegalArgumentException(msg);
            }
            this.nbQueues = size;
        }
        this.name = basePath.getName();
        this.basePath = basePath;
        queues = new ArrayList<>(this.nbQueues);
        log.info("Opening chronicle queues in: " + basePath);

        for (int i = 0; i < nbQueues; i++) {
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


}

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

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;
import static org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicle.ChronicleMQManager.DEFAULT_RETENTION_DURATION;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingResourcesCache;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.internals.MQOffsetImpl;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

/**
 * Chronicle Queue implementation of MQAppender.
 *
 * Note that for performance reason the class loader assertion are disabled.
 *
 * @since 9.1
 */
public class ChronicleMQAppender<M extends Externalizable> implements MQAppender<M>, StoreFileListener {
    private static final Log log = LogFactory.getLog(ChronicleMQAppender.class);
    private static final String QUEUE_PREFIX = "Q-";
    private static final int POLL_INTERVAL_MS = 100;

    private static final String SECOND_ROLLING_PERIOD = "s";

    private static final String MINUTE_ROLLING_PERIOD = "m";

    private static final String HOUR_ROLLING_PERIOD = "h";

    private static final String DAY_ROLLING_PERIOD = "d";

    private final List<ChronicleQueue> queues;
    private final int nbQueues;
    private final File basePath;
    private final String name;

    private int retentionNbCycles;

    // keep track of created tailers to make sure they are closed before the mq
    private final ConcurrentLinkedQueue<ChronicleMQTailer<M>> tailers = new ConcurrentLinkedQueue<>();
    private boolean closed = false;

    static public boolean exists(File basePath) {
        return basePath.isDirectory() && basePath.list().length > 0;
    }

    public String getBasePath() {
        return basePath.getPath();
    }

    /**
     * Create a new mqueues
     */
    static public <M extends Externalizable> ChronicleMQAppender<M> create(File basePath, int size,
            String retentionPolicy) {
        return new ChronicleMQAppender<>(basePath, size, retentionPolicy);
    }

    /**
     * Create a new mqueues.
     */
    static public <M extends Externalizable> ChronicleMQAppender<M> create(File basePath, int size) {
        return new ChronicleMQAppender<>(basePath, size, DEFAULT_RETENTION_DURATION);
    }

    /**
     * Open an existing mqueues.
     */
    static public <M extends Externalizable> ChronicleMQAppender<M> open(File basePath) {
        return new ChronicleMQAppender<>(basePath, 0, DEFAULT_RETENTION_DURATION);
    }

    /**
     * Open an existing mqueues.
     */
    static public <M extends Externalizable> ChronicleMQAppender<M> open(File basePath, String retentionDuration) {
        return new ChronicleMQAppender<>(basePath, 0, retentionDuration);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int size() {
        return nbQueues;
    }

    @Override
    public MQOffset append(int partition, M message) {
        ExcerptAppender appender = queues.get(partition).acquireAppender();
        appender.writeDocument(w -> w.write("msg").object(message));
        long offset = appender.lastIndexAppended();
        MQOffset ret = new MQOffsetImpl(name, partition, offset);
        if (log.isDebugEnabled()) {
            log.debug(String.format("append to %s, value: %s", ret, message));
        }
        return ret;
    }

    public MQTailer<M> createTailer(MQPartition partition, String group) {
        return addTailer(new ChronicleMQTailer<>(basePath.toString(),
                queues.get(partition.partition()).createTailer(), partition, group));
    }

    public long endOffset(int partition) {
        return queues.get(partition).createTailer().toEnd().index();
    }

    public long firstOffset(int partition) {
        long ret = queues.get(partition).firstIndex();
        if (ret == Long.MAX_VALUE) {
            return 0;
        }
        return ret;
    }

    public long countMessages(int partition, long lowerOffset, long upperOffset) {
        long ret = 0;
        SingleChronicleQueue queue = (SingleChronicleQueue) queues.get(partition);
        try {
            ret = queue.countExcerpts(lowerOffset, upperOffset);
        } catch (IllegalStateException e) {
            // 'file not found' for the lowerCycle
            return 0;
        }
        // System.out.println("partition: " + partition + ", count from " + lowerOffset + " to " + upperOffset + " = " + ret);
        return ret;
    }

    private MQTailer<M> addTailer(ChronicleMQTailer<M> tailer) {
        tailers.add(tailer);
        return tailer;
    }

    @Override
    public boolean waitFor(MQOffset offset, String group, Duration timeout) throws InterruptedException {
        boolean ret;
        long offsetPosition = offset.offset();
        int partition = offset.partition().partition();
        try (ChronicleMQOffsetTracker offsetTracker = new ChronicleMQOffsetTracker(basePath.toString(), partition, group)) {
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

    @Override
    public boolean closed() {
        return closed;
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
        closed = true;
    }

    private ChronicleMQAppender(File basePath, int size, String retentionDuration) {
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

        if (retentionDuration != null) {
            retentionNbCycles = Integer.valueOf(retentionDuration.substring(0, retentionDuration.length() - 1));
        }

        RollCycle rollCycle = getRollCycle(retentionDuration);

        queues = new ArrayList<>(this.nbQueues);
        log.debug(String.format("%s chronicle mqueue: %s, path: %s, size: %d",
                (size == 0) ? "Opening" : "Creating", name, basePath, nbQueues));

        for (int i = 0; i < nbQueues; i++) {
            File path = new File(basePath, String.format("%s%02d", QUEUE_PREFIX, i));
            ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                                                              .rollCycle(rollCycle)
                                                              .storeFileListener(this)
                                                              .build();
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

    private RollCycle getRollCycle(String retentionDuration) {
        String rollingPeriod = retentionDuration.substring(retentionDuration.length() - 1);
        RollCycle rollCycle;
        switch (rollingPeriod) {
        case SECOND_ROLLING_PERIOD:
            rollCycle = RollCycles.TEST_SECONDLY;
            break;
        case MINUTE_ROLLING_PERIOD:
            rollCycle = RollCycles.MINUTELY;
            break;
        case HOUR_ROLLING_PERIOD:
            rollCycle = RollCycles.HOURLY;
            break;
        case DAY_ROLLING_PERIOD:
            rollCycle = RollCycles.DAILY;
            break;
        default:
            String msg = "Unknown rolling period: " + rollingPeriod + " for MQueue: " + name();
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        return rollCycle;
    }

    private int findQueueIndex(File queueFile) {
        String queueDirName = queueFile.getParentFile().getName();
        return Integer.valueOf(queueDirName.substring(queueDirName.length() - 2));
    }

    @Override
    public void onAcquired(int cycle, File file) {
        if (log.isDebugEnabled()) {
            log.debug("New file created: " + file + " on cycle: " + cycle);
        }

        SingleChronicleQueue queue = (SingleChronicleQueue) queues.get(findQueueIndex(file));

        int lowerCycle = queue.firstCycle();
        int upperCycle = cycle - retentionNbCycles;

        purgeQueue(lowerCycle, upperCycle, queue);

    }

    /**
     * Files in queue older than the current date minus the retention duration are candidates for purging, knowing that
     * the more recent files should be kept to ensure no data loss (for example after an interruption longer than the
     * retention duration).
     */
    protected void purgeQueue(int lowerCycle, int upperCycle, SingleChronicleQueue queue) {
        // TODO this method should be refactored after chronicle-queue lib upgrade
        File[] files = queue.file().listFiles();

        if (files != null && lowerCycle < upperCycle) {
            RollingResourcesCache cache = new RollingResourcesCache(queue.rollCycle(), queue.epoch(),
                    name -> new File(queue.file().getAbsolutePath(), name + SUFFIX),
                    f -> FilenameUtils.removeExtension(f.getName()));

            Arrays.stream(files)
                  .sorted(Comparator.comparingLong(cache::toLong)) // Order files by cycles
                  .limit(files.length - retentionNbCycles) // Keep the 'retentionNbCycles' more recent files
                  .filter(f -> cache.parseCount(FilenameUtils.removeExtension(f.getName())) < upperCycle)
                  .forEach(f -> {
                      if (f.delete()) {
                          log.info("Queue file deleted: " + f.getAbsolutePath());
                      }
                  });
        }
    }

    @Override
    public void onReleased(int cycle, File file) {

    }
}

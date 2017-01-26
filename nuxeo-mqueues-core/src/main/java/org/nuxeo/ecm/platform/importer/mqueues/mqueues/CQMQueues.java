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
import net.openhft.chronicle.wire.ValueIn;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.apache.commons.io.FileUtils.deleteDirectory;

/**
 * Chronicle Queue implementation of MQueues
 *
 * @since 9.1
 */
public class CQMQueues<M extends Message> implements MQueues<M> {
    private static final Log log = LogFactory.getLog(CQMQueues.class);
    private static final String QUEUE_PREFIX = "Q-";
    protected final List<ChronicleQueue> queues;
    protected final List<CQTailer> tailers;
    protected final ChronicleQueue offsetQueue;
    protected final int nbQueues;
    protected final File basePath;

    public class CQTailer implements Tailer<M> {
        private final int queueNb;
        private ExcerptTailer tailer = null;

        public CQTailer(int queueNb) {
            this.queueNb = queueNb;
        }

        @Override
        public M get(long timeout, TimeUnit unit) throws InterruptedException {
            M ret = get();
            if (ret != null) {
                return ret;
            }
            final long timeoutMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
            final long deadline = System.currentTimeMillis() + timeoutMs;
            final long delay = Math.min(100, timeoutMs);
            while (ret == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(delay);
                ret = get();
            }
            return ret;
        }

        @SuppressWarnings("unchecked")
        protected M get() {
            final List<M> ret = new ArrayList<>(1);
            if (getTailer().readDocument(w -> {
                ret.add((M) w.read("msg").object());
            })) {
                return ret.get(0);
            }
            return null;
        }

        public ExcerptTailer getTailer() {
            if (tailer != null) {
                return tailer;
            }
            tailer = queues.get(queueNb).createTailer();
            toLastCommitted();
            return tailer;
        }

        @Override
        public void commit() {
            // we write raw: queue, offset
            offsetQueue.acquireAppender().writeDocument(w -> w
                    .getValueOut().int32(queueNb)
                    .getValueOut().int64(getTailer().index()));
            if (log.isTraceEnabled()) {
                log.trace(String.format("queue-%02d commit offset: %d", queueNb, tailer.index()));
            }
        }

        @Override
        public void toEnd() {
            log.debug(String.format("queue-%02d toEnd", queueNb));
            getTailer().toEnd();
        }

        @Override
        public void toStart() {
            log.debug(String.format("queue-%02d toStart", queueNb));
            getTailer().toStart();
        }

        @Override
        public void toLastCommitted() {
            log.debug(String.format("queue-%02d toLastCommitted", queueNb));
            ExcerptTailer offsetTailer = offsetQueue.createTailer().direction(TailerDirection.BACKWARD).toEnd();
            boolean stop = false;
            final boolean[] found = {false};
            while (offsetTailer.readDocument(w -> {
                ValueIn in = w.getValueIn();
                int oq = in.int32();
                long offset = in.int64();
                if (queueNb == oq) {
                    log.trace(String.format("queue-%02d found last committed index: %d", queueNb, offset));
                    getTailer().moveToIndex(offset);
                    found[0] = true;
                }
            })) {
                if (found[0]) {
                    break;
                }
            }
            if (!found[0]) {
                log.trace(String.format("queue-%02d last committed not found, start from beginning", queueNb));
                toStart();
            }
        }

        @Override
        public int getQueue() {
            return queueNb;
        }
    }

    /**
     * Open an existing mqueues
     */
    public CQMQueues(File basePath) {
        this(basePath, 0, true);
    }

    /**
     * Create a new mqueues, removing existing one if any.
     */
    public CQMQueues(File basePath, int size) {
        this(basePath, size, false);
    }

    private CQMQueues(File basePath, int size, boolean append) {
        if (!append) {
            resetBasePath(basePath);
            this.nbQueues = size;
        } else {
            this.nbQueues = getNbQueues(basePath);
        }
        queues = new ArrayList<>(this.nbQueues);
        tailers = new ArrayList<>(this.nbQueues);
        this.basePath = basePath;
        log.info("Using chronicle queues in: " + basePath);
        offsetQueue = binary(new File(basePath, "offset")).build();

        for (int i = 0; i < this.nbQueues; i++) {
            File path = new File(basePath, String.format("%s%02d", QUEUE_PREFIX, i));
            ChronicleQueue queue = binary(path).build();
            // touch the queue so we can count them even if they stay empty.
            queue.file().mkdirs();
            queues.add(queue);
            tailers.add(new CQTailer(i));
        }

        // When using big number make sure java assert is disable or CQ GC will knock at the door
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        loader.setDefaultAssertionStatus(false);
    }

    private int getNbQueues(File basePath) {
        int ret;
        try (Stream<Path> paths = Files.list(basePath.toPath())) {
            ret =  (int) paths.filter(path -> (Files.isDirectory(path) && path.getFileName().toString().startsWith(QUEUE_PREFIX))).count();
            if (ret == 0) {
                throw new IOException("No chronicles queues file found");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid basePath for queue: " + basePath, e);
        }
        return ret;
    }

    private void resetBasePath(File basePath) {
        try {
            log.info("Removing Chronicle Queues directory: " + basePath);
            deleteDirectory(basePath);
        } catch (IOException e) {
            String msg = "Can not remove Chronicle Queues directory: " + basePath + " " + e.getMessage();
            log.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }
        if (!basePath.mkdirs()) {
            String msg = "Can not create Chronicle Queues in: " + basePath;
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public int size() {
        return nbQueues;
    }

    @Override
    public void put(int queue, M message) {
        queues.get(queue).acquireAppender().writeDocument(w -> w.write("msg").object(message));
    }

    @Override
    public Tailer<M> getTailer(int queue) {
        return tailers.get(queue);
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing queue");
        queues.stream().filter(Objects::nonNull).forEach(ChronicleQueue::close);
        offsetQueue.close();
        tailers.clear();
        queues.clear();
    }

}

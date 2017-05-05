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
package org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/**
 * Track committed offset for a queue.
 *
 * @since 9.1
 */
public class CQOffsetTracker implements AutoCloseable {
    private final SingleChronicleQueue offsetQueue;
    private final int queueIndex;
    private static final String OFFSET_QUEUE_PREFIX = "offset-";
    private long lastCommittedOffset;

    public CQOffsetTracker(String basePath, int queue, String nameSpace) {
        queueIndex = queue;
        File offsetFile = new File(basePath, OFFSET_QUEUE_PREFIX + nameSpace);
        offsetQueue = binary(offsetFile).build();
    }

    /**
     * Use a cache to return the last committed offset, concurrent consumer is not taken in account
     * use {@link #readLastCommittedOffset()} in concurrency.
     */
    public long getLastCommittedOffset() {
        if (lastCommittedOffset > 0) {
            return lastCommittedOffset;
        }
        return readLastCommittedOffset();
    }

    /**
     * Read the last committed offset from the file.
     */
    public long readLastCommittedOffset() {
        ExcerptTailer offsetTailer = offsetQueue.createTailer().direction(TailerDirection.BACKWARD).toEnd();
        final long[] offset = {0};
        boolean hasNext;
        do {
            hasNext = offsetTailer.readBytes(b -> {
                int queue = b.readInt();
                long off = b.readLong();
                long stamp = b.readLong();
                if (queueIndex == queue) {
                    offset[0] = off;
                }
            });
        } while (offset[0] == 0 && hasNext);
        return offset[0];
    }

    public void commit(long offset) {
        offsetQueue.acquireAppender().writeBytes(b -> b.writeInt(queueIndex).writeLong(offset).writeLong(System.currentTimeMillis()));
        lastCommittedOffset = offset;
    }

    @Override
    public void close() {
        offsetQueue.close();
    }

}

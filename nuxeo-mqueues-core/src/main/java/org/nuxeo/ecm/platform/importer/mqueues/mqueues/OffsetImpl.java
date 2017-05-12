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

/**
 * @since 9.1
 */
public class OffsetImpl implements Offset {
    private final long offset;
    private final int queue;

    public OffsetImpl(int queue, long offset) {
        this.queue = queue;
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public int getQueue() {
        return queue;
    }

    @Override
    public String toString() {
        return String.format("OffsetImpl(%d, %d)", queue, offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetImpl offsetImpl = (OffsetImpl) o;

        if (queue != offsetImpl.queue) return false;
        return offset == offsetImpl.offset;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + queue;
        return result;
    }

    @Override
    public int compareTo(Offset o) {
        if (this == o) return 0;
        if (o == null || getClass() != o.getClass()) {
            throw new IllegalArgumentException("Can not compare offsets with different classes");
        }
        OffsetImpl offsetImpl = (OffsetImpl) o;
        if (queue != offsetImpl.queue) {
            throw new IllegalArgumentException("Can not compare offsets from different queues");
        }
        return (int) (offset - offsetImpl.offset);
    }
}

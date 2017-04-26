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
package org.nuxeo.ecm.platform.importer.mqueues.computation;

import java.util.Objects;

/**
 * Watermark represents a point in time.
 * This point in time is composed of a millisecond timestamp and a sequence.
 * There is also a state to denote if the point in time is reached (completed) or not.
 *
 * Watermark are immutable.
 *
 * @since 9.2
 */
final public class Watermark implements Comparable<Watermark> {
    final private long timestamp;
    final private short sequence;
    final private boolean completed;
    final private long value;
    final public static Watermark LOWEST = new Watermark(0, (short) 0, false);

    private Watermark(long timestamp, short sequence, boolean completed) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp must be positive");
        }
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.completed = completed;
        this.value = timestamp << 17 | (sequence & 0xFFFF) << 1 | (completed ? 1 : 0);
    }

    static public Watermark ofValue(long watermarkValue) {
        return new Watermark(
                watermarkValue >> 17,
                (short) ((watermarkValue >> 1) & 0xFFFF),
                (watermarkValue & 1L) == 1L);
    }

    static public Watermark ofTimestamp(long timestamp) {
        return ofTimestamp(timestamp, (short) 0);
    }

    static public Watermark ofTimestamp(long timestamp, short sequence) {
        return new Watermark(timestamp, sequence, false);
    }

    static public Watermark completedOf(Watermark watermark) {
        Objects.requireNonNull(watermark);
        return new Watermark(watermark.getTimestamp(), watermark.getSequence(), true);
    }

    public long getValue() {
        return value;
    }

    public boolean isCompleted() {
        return completed;
    }

    public short getSequence() {
        return sequence;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isDone(long timestamp) {
        if (this.timestamp > timestamp) {
            return true;
        }
        // here sequence are not taken in account ?
        return completed && (this.timestamp == timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Watermark watermark = (Watermark) o;
        if (completed != watermark.completed) return false;
        if (timestamp != watermark.timestamp) return false;
        return sequence == watermark.sequence;

    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "Watermark{" +
                "completed=" + completed +
                ", timestamp=" + timestamp +
                ", sequence=" + sequence +
                ", value=" + value +
                '}';
    }

    @Override
    public int compareTo(Watermark o) {
        if (o == null) {
            return Integer.MAX_VALUE;
        }
        long diff = value - o.value;
        // cast diff to int when possible
        int ret = (int) diff;
        if (ret == diff) {
            return ret;
        }
        if (diff > 0) {
            return Integer.MAX_VALUE;
        }
        return Integer.MIN_VALUE;
    }

}

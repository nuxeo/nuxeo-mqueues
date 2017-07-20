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
 * Represent the number of messages between 2 offsets
 *
 * @since 9.3
 */
public class MQLag {
    final protected long lowerOffset;
    final protected long upperOffset;
    final protected long lag;
    final protected long upper;

    public MQLag(long lowerOffset, long upperOffset) {
        this(lowerOffset, upperOffset, upperOffset - lowerOffset, upperOffset);
    }

    public MQLag(long lowerOffset, long upperOffset, long lag, long upper) {
        this.lowerOffset = lowerOffset;
        this.upperOffset = upperOffset;
        this.upper = upper;
        this.lag = lag;
    }

    /**
     * Returns the number of messages between lower and upper offsets.
     */
    public long lag() {
        return lag;
    }

    /**
     * Convert the upperOffset into a number of messages.
     */
    public long upper() {
        return upper;
    }

    /**
     * Convert the lowerOffset into a number of messages.
     */
    public long lower() {
        return upper - lag;
    }


    public long upperOffset() {
        return upperOffset;
    }

    public long lowerOffset() {
        return lowerOffset;
    }


    public static MQLag of(long lowerOffset, long upperOffset) {
        return new MQLag(lowerOffset, upperOffset);
    }

    public static MQLag of(long lag) {
        return new MQLag(0, lag, lag, lag);
    }

    @Override
    public String toString() {
        return "MQLag{" +
                "lower=" + lower() +
                ", upper=" + upper +
                ", lag=" + lag +
                ", lowerOffset=" + lowerOffset +
                ", upperOffset=" + upperOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MQLag lag1 = (MQLag) o;

        return lag == lag1.lag;
    }

    @Override
    public int hashCode() {
        return (int) (lag ^ (lag >>> 32));
    }

}

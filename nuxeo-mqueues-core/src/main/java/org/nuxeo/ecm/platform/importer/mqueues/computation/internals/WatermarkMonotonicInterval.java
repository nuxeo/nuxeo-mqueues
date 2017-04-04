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
package org.nuxeo.ecm.platform.importer.mqueues.computation.internals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;

/**
 * Keep track of minimum and maximum watermark level.
 * On checkpoint move the low watermark to the previous maximum mark.
 *
 * @since 9.1
 */
public class WatermarkMonotonicInterval {
    private static final Log log = LogFactory.getLog(WatermarkMonotonicInterval.class);

    private Watermark lowest = Watermark.LOWEST;
    private volatile Watermark low = Watermark.LOWEST;
    private Watermark high = Watermark.LOWEST;

    public WatermarkMonotonicInterval() {
    }

    public long mark(long watermarkValue) {
        return mark(Watermark.ofValue(watermarkValue));
    }

    public long mark(Watermark watermark) {
        if (low == Watermark.LOWEST) {
            low = high = watermark;
        } else if (watermark.compareTo(low) < 0) {
            if (watermark.compareTo(lowest) < 0) {
                // low watermark must increase to be monotonic
                log.trace("receive too low watermark, rejected " + watermark + " lowest: " + lowest);
                low = lowest;
            } else {
                low = watermark;
            }
        }
        if (watermark.compareTo(high) > 0) {
            high = watermark;
        }
        return low.getValue();
    }

    public long checkpoint() {
        low = high;
        high = Watermark.LOWEST;
        low = Watermark.completedOf(low);
        lowest = low;
        // System.out.println(low);
        return low.getValue();
    }

    public boolean isDone(long timestamp) {
        return low.isDone(timestamp);
    }

    public Watermark getLow() {
        return low;
    }

    public Watermark getHigh() {
        return high;
    }

    @Override
    public String toString() {
        return "WatermarkInterval{" +
                "low=" + low +
                ", high=" + high +
                '}';
    }
}

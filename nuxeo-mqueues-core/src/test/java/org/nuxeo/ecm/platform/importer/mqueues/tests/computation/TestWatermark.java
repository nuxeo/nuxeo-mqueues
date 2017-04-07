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
package org.nuxeo.ecm.platform.importer.mqueues.tests.computation;

import org.junit.Test;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.WatermarkInterval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @since 9.1
 */
public class TestWatermark {

    @Test
    public void testWmInit() {
        long t0 = System.currentTimeMillis();
        Watermark wm = Watermark.ofTimestamp(t0);
        Watermark wmBis = Watermark.ofValue(wm.getValue());
        assertEquals(wm, wmBis);
        assertEquals(t0, wmBis.getTimestamp());

        // same with a sequence
        wm = Watermark.ofTimestamp(t0, (short) 1024);
        wmBis = Watermark.ofValue(wm.getValue());
        assertEquals(wm, wmBis);
        assertEquals(t0, wmBis.getTimestamp());
        assertEquals(1024, wmBis.getSequence());
    }

    @Test
    public void testWmLimits() {

        Watermark wm = Watermark.ofTimestamp(0);
        Watermark wm2 = Watermark.ofValue(0);

        assertEquals(0, wm.getValue());
        assertEquals(0, wm2.getValue());
        assertEquals(Watermark.LOWEST, wm2);

        try {
            wm = Watermark.ofTimestamp(-10);
            fail("exception expected");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            wm = Watermark.ofValue(-10);
            fail("exception expected");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // check that the timestamp is not truncated for the next century
        long t0 = System.currentTimeMillis() + 100 * 365 * 24 * 3600 * 1000;
        wm = Watermark.ofTimestamp(t0);
        wm2 = Watermark.ofValue(wm.getValue());
        assertEquals(wm, wm2);
        assertEquals(t0, wm2.getTimestamp());

    }

    @Test
    public void testWmComparable() {
        Watermark wm0 = Watermark.ofTimestamp(2);
        Watermark wm1 = Watermark.ofTimestamp(2, (short) 1);
        Watermark wm2 = Watermark.ofTimestamp(3);
        Watermark wm2bis = Watermark.ofTimestamp(3, (short) 0);
        Watermark wm3 = Watermark.ofTimestamp(3, (short) 0);
        wm3 = Watermark.completedOf(wm3);

        assertEquals(wm0, wm0);
        assertEquals(0, wm0.compareTo(wm0));
        assertEquals(wm2, wm2bis);
        assertEquals(0, wm2bis.compareTo(wm2));

        assertTrue(wm0.compareTo(wm1) < 0);
        assertTrue(wm0.compareTo(wm2) < 0);
        assertTrue(wm0.compareTo(wm3) < 0);

        assertTrue(wm1.compareTo(wm2) < 0);
        assertTrue(wm1.compareTo(wm3) < 0);
        assertTrue(wm2.compareTo(wm3) < 0);
        assertTrue(wm2bis.compareTo(wm3) < 0);

        assertTrue(wm3.compareTo(wm2bis) > 0);
        assertTrue(wm2bis.compareTo(wm1) > 0);
        assertTrue(wm3.compareTo(wm0) > 0);

        // test case of long overflow
        wm0 = Watermark.ofValue(195409460902363138L);
        wm1 = Watermark.ofTimestamp(1);
        assertTrue(wm0.compareTo(wm1) > 0);

    }

    // WatermarkInterval tests

    @Test
    public void testWmiIsDone() {
        WatermarkInterval wmi = new WatermarkInterval();
        wmi.mark(Watermark.ofTimestamp(10));
        assertTrue(wmi.isDone(9));
        assertFalse(wmi.isDone(10));
        assertFalse(wmi.isDone(11));
        assertEquals(10, wmi.getLow().getTimestamp());
        assertEquals(10, wmi.getHigh().getTimestamp());
    }

    @Test
    public void testWmiAdjustOnNullWatermark() {
        WatermarkInterval wmi = new WatermarkInterval();
        long low = wmi.mark(200000);
        assertEquals(200000, low);
        assertTrue(wmi.getLow().getTimestamp() > 0);
    }


    @Test
    public void testWmiMarkAndCheckpoint() {
        long t0 = System.currentTimeMillis();
        long t1 = t0 + 1;
        long t2 = t0 + 2;
        long t3 = t0 + 3;

        // watermark associated with timestamp
        long w0 = Watermark.ofTimestamp(t0).getValue();
        long w1 = Watermark.ofTimestamp(t1).getValue();
        long w2 = Watermark.ofTimestamp(t2).getValue();
        long w3 = Watermark.ofTimestamp(t3).getValue();

        WatermarkInterval wmi = new WatermarkInterval();
        wmi.mark(w2);
        wmi.mark(w3);
        wmi.mark(w2);
        wmi.mark(w0);
        wmi.mark(w1);
        // the low watermark is w0
        assertEquals(w0, wmi.getLow().getValue());
        assertEquals(w3, wmi.getHigh().getValue());

        // no checkpoint occure so nothing is done
        assertFalse(wmi.getLow().isCompleted());
        assertFalse(wmi.isDone(t0));
        assertFalse(wmi.isDone(t3));

        wmi.checkpoint();
        // low watermark is now to w3 completed
        assertTrue(wmi.getLow().isCompleted());
        assertTrue(wmi.isDone(t0));
        assertTrue(wmi.isDone(t2));
        assertTrue(wmi.isDone(t3));
        assertTrue(wmi.getLow().getValue() > w3);
        assertEquals(t3, wmi.getLow().getTimestamp());

        // mark with older watermark
        wmi.mark(w0);
        assertFalse(wmi.getLow().isCompleted());
        assertEquals(w0, wmi.getLow().getValue());
        assertFalse(wmi.isDone(t0));
        assertFalse(wmi.isDone(t3));

        // checkpoint after w0
        wmi.checkpoint();
        assertTrue(wmi.getLow().isCompleted());
        assertTrue(wmi.isDone(t0));
        assertFalse(wmi.isDone(t1));
    }

    @Test
    public void testWmiMarkAndCheckpointWithSequence() {
        //long t0 = System.currentTimeMillis();
        long t0 = 1490855872363L; // fail
        // long t0 = 1490855953208L; // ok
        long t1 = t0 + 1;
        long w0 = Watermark.ofTimestamp(t0).getValue();
        long w1 = Watermark.ofTimestamp(t0, (short) 1).getValue();
        long w2 = Watermark.ofTimestamp(t0, (short) 2).getValue();
        long w3 = Watermark.ofTimestamp(t0, (short) 3).getValue();

        Watermark x = Watermark.ofValue(w0);
        WatermarkInterval wmi = new WatermarkInterval();
        wmi.mark(w2);
        wmi.mark(w0);
        wmi.mark(w1);
        wmi.mark(w3);

        assertEquals(w0, wmi.getLow().getValue());
        assertFalse(wmi.getLow().isCompleted());
        assertFalse(wmi.isDone(t0));

        wmi.checkpoint();
        assertEquals(t0, wmi.getLow().getTimestamp());
        assertTrue(wmi.isDone(t0));
        assertFalse(wmi.isDone(t1));

        // mark with a smaller wm
        wmi.mark(w1);
        assertEquals(w1, wmi.getLow().getValue());

        wmi.checkpoint();
        System.out.println(wmi);
        assertTrue(wmi.getLow().getValue() > w1);
        assertTrue(w2 > wmi.getLow().getValue());
    }


    @Test
    public void testWmiLowWatermarkWithSequenceCheckpoint() {
        long t0 = System.currentTimeMillis();
        long w0 = Watermark.ofTimestamp(t0).getValue();
        long w1 = Watermark.ofTimestamp(t0, (short) 1).getValue();
        long w2 = Watermark.ofTimestamp(t0, (short) 2).getValue();

        WatermarkInterval wmi = new WatermarkInterval();
        wmi.mark(w0);
        wmi.checkpoint();

        wmi.mark(w1);
        wmi.checkpoint();
        assertTrue(wmi.getLow().getValue() > w1);
        assertTrue(w2 > wmi.getLow().getValue());
        // bis
        wmi.mark(w1);
        wmi.checkpoint();
        assertTrue(wmi.getLow().getValue() > w1);
        assertTrue(w2 > wmi.getLow().getValue());

    }

    @Test
    public void testWmiLimits() {
        WatermarkInterval wmi = new WatermarkInterval();
        // checkpoint without mark
        wmi.checkpoint();
        assertTrue(wmi.getLow().isCompleted());

        // default low and high
        wmi = new WatermarkInterval();
        assertEquals(Watermark.LOWEST, wmi.getLow());
        assertEquals(Watermark.LOWEST, wmi.getHigh());

        // mark lowest
        wmi = new WatermarkInterval();
        wmi.mark(Watermark.LOWEST);
        long low = wmi.checkpoint();
        assertTrue(wmi.getLow().isCompleted());

        // double checkpoint
        wmi.checkpoint();
        wmi.checkpoint();
        assertTrue(wmi.getLow().isCompleted());
        assertEquals(low, wmi.getLow().getValue());
    }

}

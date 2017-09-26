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
package org.nuxeo.ecm.platform.mqueues.tests.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.platform.mqueues.workmanager.ComputationWork;
import org.nuxeo.lib.core.mqueues.computation.Record;
import org.nuxeo.lib.core.mqueues.mqueues.MQAppender;
import org.nuxeo.lib.core.mqueues.mqueues.MQLag;
import org.nuxeo.lib.core.mqueues.mqueues.MQManager;
import org.nuxeo.lib.core.mqueues.mqueues.MQOffset;
import org.nuxeo.lib.core.mqueues.mqueues.MQPartition;
import org.nuxeo.lib.core.mqueues.mqueues.MQRecord;
import org.nuxeo.lib.core.mqueues.mqueues.MQTailer;
import org.nuxeo.lib.core.mqueues.mqueues.internals.MQOffsetImpl;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Here are some useful code example for introspect and manipulate MQueue.
 *
 * @since 9.3
 */
// Uncomment to be able to deserialize Nuxeo work
@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
public abstract class TestDebugTools {
    protected static final Log log = LogFactory.getLog(TestDebugTools.class);

    public abstract MQManager createManager();

    protected MQManager manager;

    @Before
    public void initManager() throws Exception {
        if (manager == null) {
            manager = createManager();
        }
    }

    @After
    public void closeManager() throws Exception {
        if (manager != null) {
            manager.close();
        }
        manager = null;
    }

    @Test
    public void myDebugTest() throws Exception {
        // First check the MQManager connection in derived class
        // Then build the command you need

//        cat("mq-doc", 1, 0, 10, this::renderWork);
//        listConsumerLags();
//        tail("default");
//        listConsumerLags(Collections.singletonList("default"));
//        cat("default", 10);
//        cat("default", 1, 3, 10);
//        //render record as Work, this requires to load Nuxeo Framework (uncomment annotation)
//        cat("default", 1, 3, 10, this::renderWork);
//        tail("default");
//        changeConsumerOffset("default", 1, "default", 10);
//        resetConsumerOffsets("default", "default");
//        copy("default", "default-test");
    }

    /**
     * List all MQueues with their consumer groups and lags
     */
    public void listConsumerLags() {
        listConsumerLags(null);
    }

    /**
     * List MQueues with their consumer groups and lags
     */
    @SuppressWarnings("unchecked")
    public void listConsumerLags(List<String> names) {
        System.out.println("# MQueue report " + getCurrentDate());
        System.out.println(" MQManager: ```" + manager + "```\n");
        if (names == null || names.isEmpty()) {
            names = manager.listAll();
        }
        for (String name : names) {
            System.out.println("\n## MQueue: " + name);
            List<String> groups = manager.listConsumerGroups(name);
            if (groups.isEmpty()) {
                System.out.println("   No active consumer group.");
                continue;
            }
            for (String group : groups) {
                List<MQLag> lags = manager.getLagPerPartition(name, group);
                MQLag total = getTotalLag(lags);
                System.out.println(String.format("\n- Consumer Group: %s, lag: %d, processed: %d", group, total.lag(), total.lower()));
                System.out.println("\n   | Partition | Position | End | Lag |");
                System.out.println("   | ---: | ---: |  ---: | ---: |");
                int i = 0;
                for (MQLag lag : lags) {
                    System.out.println(String.format("   | %d | %d | %d | %d |",
                            i++, lag.lower(), lag.upper(), lag.lag()));
                }
                System.out.println("");
            }
        }
    }

    /**
     * Display up to {@code limit} messages of a MQueue from the beginning
     */
    public void cat(String name, int limit) throws Exception {
        cat(name, limit, this::renderMessage);
    }

    public void cat(String name, int limit, Consumer<MQRecord> render) throws Exception {
        MQTailer tailer = manager.createTailer("debug", name);
        tailer.toStart();
        displayMessage(tailer, limit, render);
        tailer.close();
    }

    /**
     * Display up to {@code limit} messages from a partition at offset.
     */
    public void cat(String name, int partition, long offset, int limit) throws Exception {
        cat(name, partition, offset, limit, this::renderMessage);
    }

    public void cat(String name, int partition, long offset, int limit, Consumer<MQRecord> render) throws Exception {
        MQPartition part = MQPartition.of(name, partition);
        MQOffset off = new MQOffsetImpl(part, offset);
        MQTailer tailer = manager.createTailer("debug", part);
        tailer.seek(off);
        displayMessage(tailer, limit, render);
        tailer.close();
    }

    /**
     * Tail from the end of a MQueue.
     */
    public void tail(String name) throws Exception {
        tail(name, this::renderWork);
    }

    public void tail(String name, Consumer<MQRecord> render) throws Exception {
        String group = "debug";
        int timeoutSeconds = 60;
        System.out.println("tail on " + name);
        MQTailer tailer = manager.createTailer(group, name);
        tailer.toEnd();
        while (true) {
            MQRecord record = tailer.read(Duration.ofSeconds(timeoutSeconds));
            if (record == null) {
                System.out.println("tail timeout, bye.");
                break;
            }
            render.accept(record);
        }
    }

    /**
     * Reset all committed position for a consumer group.
     */
    public void resetConsumerOffsets(String name, String group) throws Exception {
        MQTailer tailer = manager.createTailer(group, name);
        tailer.reset();
        tailer.close();
    }

    /**
     * Change a committed offset for a consumer
     */
    public void changeConsumerOffset(String name, int partition, String group, long newOffset) throws Exception {
        MQPartition part = MQPartition.of(name, partition);
        MQOffset offset = new MQOffsetImpl(part, newOffset);

        MQTailer tailer = manager.createTailer(group, part);
        System.out.println("Changing offset for " + group + " to " + offset);
        // we need to go one before and read a record because seek is a lazy operation
        MQOffset previous = new MQOffsetImpl(part, offset.offset() - 1);
        tailer.seek(previous);
        tailer.read(Duration.ofSeconds(1));
        MQOffset current = tailer.commit(part);
        if (offset.equals(current)) {
            System.out.println("Done");
        } else {
            System.err.println(String.format("Fail: expected: %s, actual: %s", offset, current));
        }
        tailer.close();
    }

    /**
     * Copy a MQueue, this will not copy the consumer offsets.
     */
    @SuppressWarnings("unchecked")
    public void copy(String src, String dest) throws Exception {
        if (manager.exists(dest)) {
            System.err.println("Can not copy, destination MQueue already exists: " + dest);
            return;
        }
        manager.createIfNotExists(dest, manager.getAppender(src).size());
        MQAppender appender = manager.getAppender(dest);
        MQTailer tailer = manager.createTailer("debug", src);
        int count = 0;
        while (true) {
            MQRecord<Record> record = tailer.read(Duration.ofSeconds(1));
            if (record == null) {
                System.out.println(String.format("%d message copied to %s", count, dest));
                break;
            }
            count++;
            // Add some filter or transformation
            // note that we could change the partitions on target BUT
            // when increasing the size this means changing the order as defined in source partition
            // Also note that committed offset are not duplicated
            appender.append(record.partition().partition(), record.message());
        }

    }

    // Helpers -----------------------------------------

    protected String getCurrentDate() {
        return timestampToDate(System.currentTimeMillis());
    }

    protected String timestampToDate(long timestamp) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return dateFormat.format(timestamp);
    }

    protected MQLag getTotalLag(List<MQLag> lags) {
        final long[] end = {0};
        final long[] pos = {Long.MAX_VALUE};
        final long[] lag = {0};
        final long[] endMessages = {0};
        lags.forEach(item -> {
            if (item.lowerOffset() > 0) {
                pos[0] = min(pos[0], item.lowerOffset());
            }
            end[0] = max(end[0], item.upperOffset());
            endMessages[0] += item.upper();
            lag[0] += item.lag();
        });
        return new MQLag(pos[0], end[0], lag[0], endMessages[0]);
    }

    protected void displayMessage(MQTailer tailer, int limit, Consumer<MQRecord> render) throws InterruptedException {
        int count = 0;
        do {
            MQRecord mqRecord = tailer.read(Duration.ofMillis(1000));
            if (mqRecord == null) {
                break;
            }
            count++;
            render.accept(mqRecord);
        } while (count < limit);
    }

    protected void renderMessage(MQRecord mqRecord) {
        System.out.println("### " + mqRecord);
        System.out.println("");
    }

    // This requires a Nuxeo Framework, so uncomment FeaturesRunner and CoreFeature annotations
    protected void renderWork(MQRecord mqRecord) {
        System.out.println("### " + mqRecord);
        if (mqRecord.message() instanceof Record) {
            try {
                // you may need to import other dependencies if the work class is not define in core module
                Work work = ComputationWork.deserialize(((Record) mqRecord.message()).data);
                System.out.println(String.format(" Work sched: %s\n cat: %s\n title: %s\n id: %s\n status: %s\n " +
                                "originatingUser: %s\n doc: %s\n path: %s\n class: %s\n",
                        timestampToDate(work.getSchedulingTime()), work.getCategory(), work.getTitle(), work.getId(),
                        work.getStatus(), work.getOriginatingUsername(), work.getDocument(), work.getSchedulePath(),
                        work.getClass().getName()
                ));
            } catch (Exception e) {
                System.err.println("Can not deserialize work: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

}

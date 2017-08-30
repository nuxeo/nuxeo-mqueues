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
package org.nuxeo.ecm.platform.mqueues.workmanager;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.lib.core.mqueues.computation.AbstractComputation;
import org.nuxeo.lib.core.mqueues.computation.ComputationContext;
import org.nuxeo.lib.core.mqueues.computation.Record;
import org.nuxeo.runtime.metrics.MetricsService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;


/**
 * @since 9.2
 */
public class ComputationWork extends AbstractComputation {
    protected final Timer workTimer;

    public ComputationWork(String name) {
        super(name, 1, 0);
        MetricRegistry registry = SharedMetricRegistries.getOrCreate(MetricsService.class.getName());
        workTimer = registry.timer(MetricRegistry.name("nuxeo", "works", name, "total"));
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
        Work work = deserialize(record.data);
        try {
            work.run();
        } finally {
            // TODO: catch error and propagate
            work.cleanUp(true, null);
            workTimer.update(work.getCompletionTime() - work.getStartTime(), TimeUnit.MILLISECONDS);
        }
        context.askForCheckpoint();
    }

    public static Work deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            return (Work) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Error " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    public static byte[] serialize(Work work) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(work);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            System.out.println("Error " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
}

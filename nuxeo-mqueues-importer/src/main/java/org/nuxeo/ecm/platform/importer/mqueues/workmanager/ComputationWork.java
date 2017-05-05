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
package org.nuxeo.ecm.platform.importer.mqueues.workmanager;

import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.platform.importer.mqueues.computation.AbstractComputation;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationContext;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;


/**
 * @since 9.2
 */
public class ComputationWork extends AbstractComputation {
    public ComputationWork(String name) {
        super(name, 1, 0);
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
        Work work = deserialize(record.data);
        try {
            work.run();
        } finally {
            // TODO catch error and propagate
            work.cleanUp(true, null);
        }
        context.askForCheckpoint();
    }

    @Override
    public void init(ComputationContext context) {
        super.init(context);
        try {
            // test to prevent conflict on startup
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new RuntimeException(e);
        }
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

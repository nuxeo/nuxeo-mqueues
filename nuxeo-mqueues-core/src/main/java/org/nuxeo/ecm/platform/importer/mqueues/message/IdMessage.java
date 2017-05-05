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
package org.nuxeo.ecm.platform.importer.mqueues.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Objects;

/**
 * Simple message with an Id and data payload.
 *
 * @since 9.1
 */
public class IdMessage implements Message {
    public static IdMessage POISON_PILL = new IdMessage("_POISON_PILL_", null, true, false);
    private String id;
    private byte[] data;
    private boolean poisonPill = false;
    private boolean forceBatch = false;

    protected IdMessage(String id, byte[] data, boolean poisonPill, boolean forceBatch) {
        this.id = Objects.requireNonNull(id);
        this.data = data;
        this.poisonPill = poisonPill;
        this.forceBatch = forceBatch;
    }

    static public IdMessage of(String id, byte[] data) {
        return new IdMessage(id, data, false, false);
    }

    static public IdMessage of(String id) {
        return new IdMessage(id, null, false, false);
    }

    /**
     * A message that force the batch.
     */
    static public IdMessage ofForceBatch(String id, byte[] data) {
        return new IdMessage(id, data, false, true);
    }

    static public IdMessage ofForceBatch(String id) {
        return new IdMessage(id, null, false, true);
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean poisonPill() {
        return poisonPill;
    }

    @Override
    public boolean forceBatch() {
        return forceBatch;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(id);
        out.writeBoolean(poisonPill);
        out.writeBoolean(forceBatch);
        if (data == null || data.length == 0) {
            out.writeInt(0);
        } else {
            out.writeInt(data.length);
            out.write(data);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.id = (String) in.readObject();
        this.poisonPill = in.readBoolean();
        this.forceBatch = in.readBoolean();
        int dataLength = in.readInt();
        if (dataLength == 0) {
            this.data = null;
        } else {
            int read = in.read(this.data, 0, dataLength);
            assert (read == dataLength);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdMessage idMessage = (IdMessage) o;

        if (poisonPill != idMessage.poisonPill) return false;
        if (forceBatch != idMessage.forceBatch) return false;
        return id != null ? id.equals(idMessage.id) : idMessage.id == null;

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + Arrays.hashCode(data);
        result = 31 * result + (poisonPill ? 1 : 0);
        result = 31 * result + (forceBatch ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("IdMessage(%s, %d, %b, %b", id, (data != null) ? data.length : 0, poisonPill, forceBatch);
    }
}

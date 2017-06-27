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
package org.nuxeo.ecm.platform.importer.mqueues.pattern;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Objects;

/**
 * Simple key value message.
 *
 * @since 9.1
 */
public class keyValueMessage implements Message {
    // Externalizable do rely on serialVersionUID
    static final long serialVersionUID = 20170529L;
    public static keyValueMessage POISON_PILL = new keyValueMessage("_POISON_PILL_", null, true, false);

    private String key;
    private byte[] value;
    private boolean poisonPill = false;
    private boolean forceBatch = false;

    public keyValueMessage() {
    }

    protected keyValueMessage(String key, byte[] value, boolean poisonPill, boolean forceBatch) {
        this.key = Objects.requireNonNull(key);
        this.value = value;
        this.poisonPill = poisonPill;
        this.forceBatch = forceBatch;
    }

    static public keyValueMessage of(String key, byte[] value) {
        return new keyValueMessage(key, value, false, false);
    }

    static public keyValueMessage of(String key) {
        return new keyValueMessage(key, null, false, false);
    }

    /**
     * A message that force the batch.
     */
    static public keyValueMessage ofForceBatch(String key, byte[] value) {
        return new keyValueMessage(key, value, false, true);
    }

    static public keyValueMessage ofForceBatch(String key) {
        return new keyValueMessage(key, null, false, true);
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    @Override
    public String getId() {
        return key;
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
        out.writeObject(key);
        out.writeBoolean(poisonPill);
        out.writeBoolean(forceBatch);
        if (value == null || value.length == 0) {
            out.writeInt(0);
        } else {
            out.writeInt(value.length);
            out.write(value);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.key = (String) in.readObject();
        this.poisonPill = in.readBoolean();
        this.forceBatch = in.readBoolean();
        int dataLength = in.readInt();
        if (dataLength == 0) {
            this.value = null;
        } else {
            this.value = new byte[dataLength];
            // not using in.readFully because it is not impl by Chronicle WireObjectInput
            int pos = 0;
            while (pos < dataLength) {
                pos += in.read(this.value, pos, dataLength - pos);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        keyValueMessage keyValueMessage = (keyValueMessage) o;

        if (poisonPill != keyValueMessage.poisonPill) return false;
        if (forceBatch != keyValueMessage.forceBatch) return false;
        return key != null ? key.equals(keyValueMessage.key) : keyValueMessage.key == null;

    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + (poisonPill ? 1 : 0);
        result = 31 * result + (forceBatch ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("keyValueMessage(\"%s\", len:%d%s%s)", key, (value != null) ? value.length : 0,
                poisonPill ? ", poison" : "", forceBatch ? ", batch" : "");
    }
}

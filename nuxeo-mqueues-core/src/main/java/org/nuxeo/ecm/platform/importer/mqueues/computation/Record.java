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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 9.2
 */
public class Record implements Externalizable {
    private static final EnumSet<Flag> DEFAULT_FLAG = EnumSet.of(Flag.DEFAULT);
    public long watermark;
    public EnumSet<Flag> flags;
    public String key;
    public byte[] data;

    public enum Flag {
        DEFAULT, BEGIN, COMMIT, POISON_PILL, INTERNAL;

        public static final EnumSet<Flag> ALL_OPTS = EnumSet.allOf(Flag.class);
    }

    static public Record of(String key, byte[] data) {
        return new Record(key, data, 0, null);
    }

    public Record(String key, byte[] data, long watermark, EnumSet<Flag> flags) {
        this.key = key;
        this.data = data;
        this.watermark = watermark;
        this.flags = flags;
    }

    @Override
    public String toString() {
        return "Record{" +
                "watermark=" + watermark +
                ", flags=" + flags +
                ", key='" + key + '\'' +
                ", data.length=" + ((data == null) ? 0 : data.length) +
                '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(watermark);
        out.writeShort(encodeFlags());
        out.writeObject(key);
        if (data == null || data.length == 0) {
            out.writeInt(0);
        } else {
            out.writeInt(data.length);
            out.write(data);
        }
    }

    private short encodeFlags() {
        // adapted from Adamski: http://stackoverflow.com/questions/2199399/storing-enumset-in-a-database
        short ret = 0;
        if (flags != null) {
            for (Flag val : flags) {
                ret |= (1 << val.ordinal());
            }
        }
        return ret;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.watermark = in.readLong();
        this.flags = decodeFlags(in.readShort());
        this.key = (String) in.readObject();
        int dataLength = in.readInt();
        if (dataLength == 0) {
            this.data = null;
        } else {
            this.data = new byte[dataLength];
            int read = in.read(this.data, 0, dataLength);
            assert (read == dataLength);
        }
    }

    private EnumSet<Flag> decodeFlags(short encoded) {
        // adapted from Adamski: http://stackoverflow.com/questions/2199399/storing-enumset-in-a-database
        Map<Integer, Flag> ordinalMap = new HashMap<>();
        for (Flag val : EnumSet.allOf(Flag.class)) {
            ordinalMap.put(val.ordinal(), val);
        }
        EnumSet<Flag> ret = EnumSet.noneOf(Flag.class);
        int ordinal = 0;
        for (int i = 1; i != 0; i <<= 1) {
            if ((i & encoded) != 0) {
                ret.add(ordinalMap.get(ordinal));
            }
            ++ordinal;
        }
        return ret;
    }

}

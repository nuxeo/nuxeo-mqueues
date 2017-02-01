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

/**
 * Simple message that contains just an identifier.
 *
 * @since 9.1
 */
public class IdMessage implements Message {
    private String id;
    private boolean poisonPill = false;
    private boolean forceBatch = false;

    public IdMessage(String id) {
        this.id = id;
    }

    public IdMessage(String id, boolean poisonPill, boolean forceBatch) {
        this.id = id;
        this.poisonPill = poisonPill;
        this.forceBatch = forceBatch;
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
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.id = (String) in.readObject();
        this.poisonPill = in.readBoolean();
        this.forceBatch = in.readBoolean();
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
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (poisonPill ? 1 : 0);
        result = 31 * result + (forceBatch ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("IdMessage(%s, %b, %b", id, poisonPill, forceBatch);
    }
}

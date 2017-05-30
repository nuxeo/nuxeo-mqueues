/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.message;

import org.nuxeo.ecm.platform.importer.mqueues.pattern.Message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A message holding info to build a StringBlob.
 *
 * @since 9.1
 */
public class BlobMessage implements Message {
    static final long serialVersionUID = 20170529L;
    private String mimetype;
    private String encoding;
    private String filename;
    private String path;
    private String content;

    public BlobMessage() {
    }

    private BlobMessage(StringMessageBuilder builder) {
        mimetype = builder.mimetype;
        encoding = builder.encoding;
        filename = builder.filename;
        path = builder.path;
        content = builder.content;
        if ((path == null || path.isEmpty()) &&
                (content == null) || content.isEmpty()) {
            throw new IllegalArgumentException("BlobMessage must be initialized with a file path or content");
        }
    }

    @Override
    public String getId() {
        return filename;
    }

    public String getMimetype() {
        return mimetype;
    }

    public String getFilename() {
        return filename;
    }

    public String getContent() {
        return content;
    }

    public String getPath() {
        return path;
    }

    public String getEncoding() {
        return encoding;
    }

    public static class StringMessageBuilder {
        private String mimetype;
        private String encoding;
        private String filename;
        private String path;
        private String content;

        /**
         * Create a string blob with a content
         *
         */
        public StringMessageBuilder(String content) {
            this.content = content;
        }

        /**
         * Set the name of the file.
         */
        public StringMessageBuilder setFilename(String filename) {
            this.filename = filename;
            return this;
        }

        /**
         * Set the path of the file containing the blob content.
         */
        public StringMessageBuilder setEncoding(String encoding) {
            this.encoding = encoding;
            return this;
        }


        /**
         * Set the mime-type of the file.
         */
        public StringMessageBuilder setMimetype(String mimetype) {
            this.mimetype = mimetype;
            return this;
        }

        protected StringMessageBuilder setPath(String path) {
            this.path = path;
            // either a path or a content not both
            this.content = null;
            return this;
        }

        public BlobMessage build() {
            return new BlobMessage(this);
        }
    }

    public static class FileMessageBuilder extends StringMessageBuilder {
        /**
         * Create a blob from a file
         *
         */
        public FileMessageBuilder(String path) {
            super(null);
            this.setPath(path);
        }

    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(mimetype);
        out.writeObject(encoding);
        out.writeObject(filename);
        out.writeObject(path);
        out.writeObject(content);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        mimetype = (String) in.readObject();
        encoding = (String) in.readObject();
        filename = (String) in.readObject();
        path = (String) in.readObject();
        content = (String) in.readObject();
    }

    //TODO: impl hashCode, equals, toString
}


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
 *
 * Contributors:
 *     bdelbosc
 */
package org.nuxeo.ecm.platform.mqueues;

import org.nuxeo.common.xmap.annotation.XNode;
import org.nuxeo.common.xmap.annotation.XNodeList;
import org.nuxeo.common.xmap.annotation.XNodeMap;
import org.nuxeo.common.xmap.annotation.XObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@XObject("topology")
public class TopologyDescriptor {
    public static final Integer DEFAULT_CONCURRENCY = 4;

    @XNode("@name")
    public String name;

    @XNode("@config")
    public String config;

    @XNode("@class")
    public Class<? extends Computations> klass;

    @XNode("@defaultConcurrency")
    public Integer defaultConcurrency = DEFAULT_CONCURRENCY;

    @XNode("@defaultPartitions")
    public Integer defaultPartitions = DEFAULT_CONCURRENCY;

    @XNodeMap(value = "option", key = "@name", type = HashMap.class, componentType = String.class)
    public Map<String, String> options = new HashMap<>();

    public String getName() {
        return name;
    }

    @XObject(value = "computation")
    public static class ComputationDescriptor {
        public ComputationDescriptor() {
        }

        @XNode("@name")
        public String name;

        @XNode("@concurrency")
        public Integer concurrency = DEFAULT_CONCURRENCY;
    }

    @XObject(value = "stream")
    public static class StreamDescriptor {

        public StreamDescriptor() {
        }

        @XNode("@name")
        public String name;

        @XNode("@partitions")
        public Integer partitions = DEFAULT_CONCURRENCY;
    }

    @XNodeList(value = "computation", type = ArrayList.class, componentType = ComputationDescriptor.class)
    public List<ComputationDescriptor> computations = new ArrayList<>(0);

    @XNodeList(value = "stream", type = ArrayList.class, componentType = StreamDescriptor.class)
    public List<StreamDescriptor> streams = new ArrayList<>(0);
}

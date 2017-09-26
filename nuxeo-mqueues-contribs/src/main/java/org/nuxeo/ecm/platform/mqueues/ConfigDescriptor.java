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

@XObject("config")
public class ConfigDescriptor {

    @XNode("@name")
    public String name;

    @XNode("@type")
    public String type;

    @XNodeMap(value = "option", key = "@name", type = HashMap.class, componentType = String.class)
    public Map<String, String> options = new HashMap<>();

    @XObject(value = "mqueue")
    public static class MQueueDescriptor {
        public static final Integer DEFAULT_PARTITIONS = 4;

        public MQueueDescriptor() {
        }

        @XNode("@name")
        public String name;

        @XNode("@size")
        public Integer size = DEFAULT_PARTITIONS;
    }

    @XNodeList(value = "mqueue", type = ArrayList.class, componentType = MQueueDescriptor.class)
    public List<MQueueDescriptor> mQueues = new ArrayList<>(0);

    public String getName() {
        return name;
    }

    public String getType() {
        if ("kafka".equalsIgnoreCase(type)) {
            return "kafka";
        }
        return "chronicle";
    }

    public String getOption(String key, String defaultValue) {
        return options.getOrDefault(key, defaultValue);
    }

    public Map<String, Integer> getMQueuesToCreate() {
        Map<String, Integer> ret = new HashMap<>();
        mQueues.forEach(d -> ret.put(d.name, d.size));
        return ret;
    }

}

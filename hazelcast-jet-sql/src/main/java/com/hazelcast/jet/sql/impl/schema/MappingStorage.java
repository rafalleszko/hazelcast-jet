/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.Map;
import java.util.stream.Stream;

public class MappingStorage {

    private static final String CATALOG_MAP_NAME = "__sql.catalog";

    private final NodeEngine nodeEngine;

    public MappingStorage(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    void put(String name, Mapping mapping) {
        storage().put(name, mapping);
    }

    boolean putIfAbsent(String name, Mapping mapping) {
        return storage().putIfAbsent(name, mapping) == null;
    }

    Stream<Mapping> values() {
        return storage().values().stream();
    }

    boolean remove(String name) {
        return storage().remove(name) != null;
    }

    private Map<String, Mapping> storage() {
        return nodeEngine.getHazelcastInstance().getReplicatedMap(CATALOG_MAP_NAME);
    }
}

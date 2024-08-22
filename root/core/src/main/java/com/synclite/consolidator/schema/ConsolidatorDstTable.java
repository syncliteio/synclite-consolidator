/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.consolidator.schema;

import java.util.concurrent.ConcurrentHashMap;

public class ConsolidatorDstTable extends Table {
    private static final ConcurrentHashMap<TableID, ConsolidatorDstTable> consolidatorDstTables = new ConcurrentHashMap<TableID, ConsolidatorDstTable>();

    private ConsolidatorDstTable(TableID id) {
        this.id = id;
    }

    public static ConsolidatorDstTable from(TableID id) {
        return consolidatorDstTables.computeIfAbsent(id, s -> new ConsolidatorDstTable(s));
    }

    public static void remove(TableID id) {
        consolidatorDstTables.remove(id);
    }

    public static int getCount() {
        return consolidatorDstTables.size();
    }

    public static void resetAll() {
        consolidatorDstTables.clear();
    }

}

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

public class TableID {
    public String deviceUUID;
    public String deviceName;
    public int dstIndex;
    public String database;
    public String schema;
    public String table;

    private static final ConcurrentHashMap<String, TableID> tableIDs = new ConcurrentHashMap<String, TableID>();
    private TableID(String device, String deviceName, int dstIndex, String database, String schema, String table) {
        this.deviceUUID = device;
        this.deviceName = deviceName;
        this.dstIndex = dstIndex;
        this.database = database;
        this.schema = schema;
        this.table = table;
    }

    public static String getResolvedName(String deviceUUID, String deviceName, int dstIndex, String database, String schema, String table) {
    	//
    	//TODO support case sensitivity in table names ?
    	//
    	StringBuilder resolvedName = new StringBuilder();
        resolvedName.append(deviceUUID);
        resolvedName.append(".");
        resolvedName.append(deviceName);
        resolvedName.append(".");
        resolvedName.append(dstIndex);
        resolvedName.append(".");       
        resolvedName.append((database != null) ? database.toLowerCase() : database);
        resolvedName.append(".");
        resolvedName.append((schema != null) ? schema.toLowerCase() : schema);
        resolvedName.append(".");
        resolvedName.append((table != null) ? table.toLowerCase() : table);
        return resolvedName.toString();
    }

    public static TableID from(String deviceUUID, String deviceName, int dstIndex, String database, String schema, String table) {
        String resolvedName = getResolvedName(deviceUUID, deviceName, dstIndex, database, schema, table);
        return tableIDs.computeIfAbsent(resolvedName, s -> new TableID(deviceUUID, deviceName, dstIndex, database, schema, table));
    }

    @Override
    public String toString() {
        return database + "." + schema + "." + table;
    }
}

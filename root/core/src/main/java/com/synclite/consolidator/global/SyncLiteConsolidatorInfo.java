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

package com.synclite.consolidator.global;

import java.nio.file.Path;

import com.synclite.consolidator.schema.TableID;

public class SyncLiteConsolidatorInfo {

    public static TableID getCheckpointTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, getSyncLiteCheckpointTableName());
    }

    public static String getSyncLiteCheckpointTableName() {
    	return "synclite_checkpoint";
    }

    /**
     * Name of the unified per-table metadata bookkeeping table on the destination.
     * Per-table source DDL is stored as {@code prop_key='create_sql'} rows here.
     */
    public static String getConsolidatorTableMetadataTableName() {
    	return "synclite_consolidator_table_metadata";
    }

    public static String getCreateConsolidatorTableMetadataTableSql() {
    	return "CREATE TABLE IF NOT EXISTS synclite_consolidator_table_metadata("
    		+ "device_uuid VARCHAR(64) NOT NULL, "
    		+ "device_name VARCHAR(255) NOT NULL, "
    		+ "database_name VARCHAR(255) NOT NULL, "
    		+ "table_name VARCHAR(255) NOT NULL, "
    		+ "prop_key VARCHAR(255) NOT NULL, "
    		+ "prop_value TEXT, "
    		+ "PRIMARY KEY(device_uuid, device_name, database_name, table_name, prop_key))";
    }

    /** Reserved {@code prop_key} value carrying the per-table CREATE SQL blob. */
    public static String getCreateSqlPropKey() {
    	return "create_sql";
    }

    public static String getMetadataFileName(int dstIndex) {
        return "synclite_consolidator_metadata_" + dstIndex + ".db";
    }

    public static String getMetadataFileSuffix(int dstIndex) {
        return "synclite_consolidator_metadata_" + dstIndex + ".db";
    }
    
    public static Path getStatsFilePath(Path dbPath, String dbName) {
        return Path.of(dbPath.toString(), "synclite_device_statistics.db");
    }
    
    public static String getSnapshotPathPrefix(Path dbPath, String dbName) {
        return Path.of(dbPath.toString(), dbName + ".snapshot.").toString();
    }

    public static Path getNextSnapshotPath(Path dbPath, String dbName, long ts) {
        return Path.of(getSnapshotPathPrefix(dbPath, dbName) + ts);
    }


    public static Path getDeviceReplicaPath(Path dbPath, String dbName, int dstIndex) {
        return Path.of(dbPath.toString(), dbName + ".synclite.backup");
    }

}

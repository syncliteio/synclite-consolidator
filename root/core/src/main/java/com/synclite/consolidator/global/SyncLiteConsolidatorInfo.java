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
import java.sql.JDBCType;

import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.DataType;
import com.synclite.consolidator.schema.StorageClass;
import com.synclite.consolidator.schema.TableID;

public class SyncLiteConsolidatorInfo {

    public static TableID getCheckpointTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, getSyncLiteCheckpointTableName());
    }

    public static ConsolidatorSrcTable getCheckpointTableSchema(String deviceUUID, String deviceName, int dstIndex) {
        ConsolidatorSrcTable checkpoint = ConsolidatorSrcTable.from(getCheckpointTableID(deviceUUID, deviceName, dstIndex));
        checkpoint.clearColumns();
        checkpoint.setIsSystemTable();
        checkpoint.addColumn(new Column(0, "commit_id", new DataType("LONG", JDBCType.BIGINT, StorageClass.NUMERIC), 1, null, 1, 0));
        checkpoint.addColumn(new Column(1, "cdc_change_number", new DataType("LONG", JDBCType.BIGINT, StorageClass.NUMERIC), 1, null, 0, 0));
        checkpoint.addColumn(new Column(2, "cdc_log_segment_sequence_number", new DataType("LONG", JDBCType.BIGINT, StorageClass.NUMERIC), 1, null, 0, 0));
        checkpoint.addColumn(new Column(3, "initialization_status", new DataType("LONG", JDBCType.BIGINT, StorageClass.NUMERIC), 1, "0", 0, 0));
        checkpoint.addColumn(new Column(4, "txn_count", new DataType("LONG", JDBCType.BIGINT, StorageClass.NUMERIC), 1, null, 0, 0));
        return checkpoint;
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

    public static TableID getConsolidatorTableMetadataTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, getConsolidatorTableMetadataTableName());
    }

    public static ConsolidatorSrcTable getConsolidatorTableMetadataTableSchema(String deviceUUID, String deviceName, int dstIndex) {
        ConsolidatorSrcTable tableMetadata = ConsolidatorSrcTable.from(getConsolidatorTableMetadataTableID(deviceUUID, deviceName, dstIndex));
        tableMetadata.clearColumns();
        tableMetadata.setIsSystemTable();
        tableMetadata.addColumn(new Column(0, "database_name", new DataType("varchar(255)", JDBCType.VARCHAR, StorageClass.TEXT), 1, null, 1, 0));
        tableMetadata.addColumn(new Column(1, "table_name", new DataType("varchar(255)", JDBCType.VARCHAR, StorageClass.TEXT), 1, null, 1, 0));
        tableMetadata.addColumn(new Column(2, "prop_key", new DataType("varchar(255)", JDBCType.VARCHAR, StorageClass.TEXT), 1, null, 1, 0));
        tableMetadata.addColumn(new Column(3, "prop_value", new DataType("text", JDBCType.LONGVARCHAR, StorageClass.TEXT), 0, null, 0, 0));
        return tableMetadata;
    }

    public static String getConsolidatorMetadataTableName() {
        return "synclite_consolidator_metadata";
    }

    public static boolean isSystemMetadataTable(String tableName) {
        if (tableName == null) {
            return false;
        }
        return getSyncLiteCheckpointTableName().equalsIgnoreCase(tableName)
                || getConsolidatorTableMetadataTableName().equalsIgnoreCase(tableName)
                || getConsolidatorMetadataTableName().equalsIgnoreCase(tableName);
    }

    public static TableID getConsolidatorMetadataTableID(String deviceUUID, String deviceName, int dstIndex) {
        return TableID.from(deviceUUID, deviceName, dstIndex, "main", null, getConsolidatorMetadataTableName());
    }

    public static ConsolidatorSrcTable getConsolidatorMetadataTableSchema(String deviceUUID, String deviceName, int dstIndex) {
        ConsolidatorSrcTable metadata = ConsolidatorSrcTable.from(getConsolidatorMetadataTableID(deviceUUID, deviceName, dstIndex));
        metadata.clearColumns();
        metadata.setIsSystemTable();
        metadata.addColumn(new Column(0, "prop_key", new DataType("varchar(255)", JDBCType.VARCHAR, StorageClass.TEXT), 1, null, 1, 0));
        metadata.addColumn(new Column(1, "prop_value", new DataType("text", JDBCType.LONGVARCHAR, StorageClass.TEXT), 0, null, 0, 0));
        return metadata;
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

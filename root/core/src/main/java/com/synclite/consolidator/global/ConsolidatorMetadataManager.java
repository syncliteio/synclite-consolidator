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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.synclite.consolidator.connector.JDBCConnector;
import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.oper.CreateTable;
import com.synclite.consolidator.oper.Delete;
import com.synclite.consolidator.oper.Insert;
import com.synclite.consolidator.processor.SQLExecutor;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.DataType;
import com.synclite.consolidator.schema.SQLGenerator;
import com.synclite.consolidator.schema.TableID;
import com.synclite.consolidator.schema.TableMapper;
import com.synclite.consolidator.watchdog.Monitor;

public class ConsolidatorMetadataManager extends MetadataManager {
    private static String createSchemaTblSql = "CREATE TABLE IF NOT EXISTS schema(database_name TEXT, schema_name TEXT, table_name TEXT, column_index LONG, column_name TEXT, column_type TEXT, column_not_null INTEGER, column_default_value BLOB, column_primary_key INTEGER, column_auto_increment INTEGER);";
    private static String deleteSchemaTblSql = "DELETE FROM schema WHERE database_name = ? AND table_name =  ?";
    private static String insertSchemaTblSql = "INSERT INTO schema (database_name, schema_name, table_name, column_index, column_name, column_type, column_not_null, column_default_value, column_primary_key, column_auto_increment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static String selectSchemaTblSql = "SELECT database_name, schema_name, table_name, column_index, column_name, column_type, column_not_null, column_default_value, column_primary_key, column_auto_increment FROM schema ORDER BY database_name, schema_name, table_name";

    private static String createTableMetadataTblSql = "CREATE TABLE IF NOT EXISTS table_metadata(database_name TEXT, schema_name TEXT, table_name TEXT, key TEXT, value TEXT);";
    private static String deleteTableMetadataTblSql = "DELETE FROM table_metadata WHERE database_name = ? AND table_name =  ? AND key = ?";
    private static String insertTableMetadataTblSql = "INSERT INTO table_metadata(database_name, schema_name, table_name, key, value) VALUES (?, ?, ?, ?, ?)";

    private ConcurrentHashMap<TableID, ConsolidatorSrcTable> deviceSrcTables = new ConcurrentHashMap<>();
    private final Device device;
    private long initializationStatus;
    private String consolidatedSnapshotName;
    private long initializationCount;
    private volatile long lastConsolidatedCDCLogSegmentSeqNumber = -1;
    private int dstIndex;
        private final TableMapper systemTableMapper;
        private final ConsolidatorSrcTable dstMetadataSystemTable;
        private final ConsolidatorSrcTable dstTableMetadataSystemTable;

    protected ConsolidatorMetadataManager(Path metadataFilePath, Device device, int dstIndex) throws SQLException {
        super(metadataFilePath);
        this.device = device;
        this.dstIndex = dstIndex;
        try {
            this.systemTableMapper = TableMapper.getSystemTableMapperInstance(dstIndex);
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to initialize system table mapper", e);
        }
        this.dstMetadataSystemTable = SyncLiteConsolidatorInfo.getConsolidatorMetadataTableSchema(device.getDeviceUUID(), device.getDeviceName(), dstIndex);
        this.dstTableMetadataSystemTable = SyncLiteConsolidatorInfo.getConsolidatorTableMetadataTableSchema(device.getDeviceUUID(), device.getDeviceName(), dstIndex);
        initializeSchemaTable();        
    }

    private void initializeSchemaTable() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createSchemaTblSql);
                stmt.execute(createTableMetadataTblSql);
            }
        }
    }

    public Object getTableMetadataEntry(ConsolidatorSrcTable srcTable, String key) throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT value FROM table_metadata WHERE database_name = '" + srcTable.id.database + "' AND table_name = '" + srcTable.id.table + "' AND key = '" + key + "'")) {
                        if (rs.next()) {
                            return rs.getObject(1);
                        }
                    }
                }
            }
            return null;
        }
        // DESTINATION mode: read from destination using a single autoCommit=true
        // connection to avoid the two-connection DDL-lock deadlock.
        try (Connection conn = JDBCConnector.getInstance(dstIndex).connect()) {
            conn.setAutoCommit(true);
            String tblMetaTbl = qualifiedDstTableName("synclite_consolidator_table_metadata");
            if (!dstTableMetadataTableEnsured) {
                try (Statement ddl = conn.createStatement()) {
                    ddl.execute("CREATE TABLE IF NOT EXISTS " + tblMetaTbl
                            + "(synclite_device_id VARCHAR(64) NOT NULL,"
                            + " synclite_device_name VARCHAR(255) NOT NULL,"
                            + " synclite_update_timestamp TEXT,"
                            + " database_name VARCHAR(255) NOT NULL,"
                            + " table_name VARCHAR(255) NOT NULL,"
                            + " prop_key VARCHAR(255) NOT NULL,"
                            + " prop_value TEXT,"
                            + " PRIMARY KEY(synclite_device_id, synclite_device_name, database_name, table_name, prop_key))");
                }
                dstTableMetadataTableEnsured = true;
            }
            String sql = "SELECT prop_value FROM " + tblMetaTbl + " WHERE synclite_device_id = ? AND synclite_device_name = ? AND database_name = ? AND table_name = ? AND prop_key = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, this.device.getDeviceUUID());
                stmt.setString(2, this.device.getDeviceName());
                stmt.setString(3, srcTable.id.database);
                stmt.setString(4, srcTable.id.table);
                stmt.setString(5, key);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getObject(1);
                    }
                }
            }
            return null;
        } catch (DstExecutionException e) {
            throw new SQLException("Failed to connect to destination for table metadata lookup", e);
        }
    }

    public void deleteTableMetadataInfo() throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DELETE FROM table_metadata");
                }
            }
            return;
        }
        // DESTINATION mode: delete from destination table
        try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
            dstExecutor.beginTran();
            ensureDstTableMetadataTable(dstExecutor);
            seedDstMetadataVersionIfAbsent(dstExecutor);
            Delete deleteAll = new Delete(dstTableMetadataSystemTable, new ArrayList<Object>());
            deleteAll.whereColumns = new ArrayList<Column>();
            dstExecutor.execute(systemTableMapper.mapOper(deleteAll));
            dstExecutor.commitTran();
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to connect to destination to delete table metadata", e);
        }
    }

    public void deleteSchemaInfo() throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DELETE FROM schema");
                }
            }
        } else {
            // DESTINATION mode: delete create_sql rows from unified table_metadata
            try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
                dstExecutor.beginTran();
                ensureDstTableMetadataTable(dstExecutor);
                seedDstMetadataVersionIfAbsent(dstExecutor);
                ArrayList<Object> beforeValues = new ArrayList<Object>(1);
                beforeValues.add(SyncLiteConsolidatorInfo.getCreateSqlPropKey());
                Delete deleteSchemaRows = new Delete(dstTableMetadataSystemTable, beforeValues);
                deleteSchemaRows.whereColumns = new ArrayList<Column>();
                deleteSchemaRows.whereColumns.add(dstTableMetadataSystemTable.colMap.get("prop_key"));
                dstExecutor.execute(systemTableMapper.mapOper(deleteSchemaRows));
                dstExecutor.commitTran();
            } catch (SyncLiteException e) {
                throw new SQLException("Failed to connect to destination to delete schema info", e);
            }
        }
        this.deviceSrcTables.clear();
    }

    public void upsertTableMetadataEntry(ConsolidatorSrcTable srcTable, String key, Object value) throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                conn.setAutoCommit(false);
                try (PreparedStatement deleteStmt = conn.prepareStatement(deleteTableMetadataTblSql)) {
                    deleteStmt.setString(1, srcTable.id.database);
                    deleteStmt.setString(2, srcTable.id.table);
                    deleteStmt.setString(3, key);
                    deleteStmt.execute();
                }
                try (PreparedStatement insertStmt = conn.prepareStatement(insertTableMetadataTblSql)) {
                    insertStmt.setString(1, srcTable.id.database);
                    insertStmt.setString(2, srcTable.id.schema);
                    insertStmt.setString(3, srcTable.id.table);
                    insertStmt.setString(4, key);
                    insertStmt.setObject(5, value);
                    insertStmt.execute();
                }
                conn.commit();
            }
            return;
        }
        // DESTINATION mode: write to destination
        try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
            dstExecutor.beginTran();
            ensureDstTableMetadataTable(dstExecutor);
            seedDstMetadataVersionIfAbsent(dstExecutor);

            ArrayList<Object> beforeValues = new ArrayList<Object>(3);
            beforeValues.add(srcTable.id.database);
            beforeValues.add(srcTable.id.table);
            beforeValues.add(key);
            Delete deleteEntry = new Delete(dstTableMetadataSystemTable, beforeValues);
            deleteEntry.whereColumns = new ArrayList<Column>();
            deleteEntry.whereColumns.add(dstTableMetadataSystemTable.colMap.get("database_name"));
            deleteEntry.whereColumns.add(dstTableMetadataSystemTable.colMap.get("table_name"));
            deleteEntry.whereColumns.add(dstTableMetadataSystemTable.colMap.get("prop_key"));
            dstExecutor.execute(systemTableMapper.mapOper(deleteEntry));

            ArrayList<Object> afterValues = new ArrayList<Object>(4);
            afterValues.add(srcTable.id.database);
            afterValues.add(srcTable.id.table);
            afterValues.add(key);
            afterValues.add(value == null ? null : value.toString());
            dstExecutor.execute(systemTableMapper.mapOper(new Insert(dstTableMetadataSystemTable, afterValues, false)));
            dstExecutor.commitTran();
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to connect to destination for table metadata persistence", e);
        }
    }

    public void upsertSchema(ConsolidatorSrcTable srcTable) throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                conn.setAutoCommit(false);
                try (PreparedStatement deleteSchemaStmt = conn.prepareStatement(deleteSchemaTblSql)) {
                    deleteSchemaStmt.setString(1, srcTable.id.database);
                    deleteSchemaStmt.setString(2, srcTable.id.table);
                    deleteSchemaStmt.execute();
                }

                try (PreparedStatement insertSchemaStmt = conn.prepareStatement(insertSchemaTblSql)) {
                    insertSchemaStmt.setString(1, srcTable.id.database);
                    insertSchemaStmt.setNull(2, Types.NULL);
                    insertSchemaStmt.setString(3, srcTable.id.table);
                    for (Column c : srcTable.columns) {
                        insertSchemaStmt.setLong(4, c.cid);
                        insertSchemaStmt.setString(5, c.column);
                        insertSchemaStmt.setString(6, c.type.dbNativeDataType);
                        insertSchemaStmt.setInt(7, c.isNotNull);
                        insertSchemaStmt.setString(8, c.defaultValue);
                        insertSchemaStmt.setInt(9, c.pkIndex);
                        insertSchemaStmt.setInt(10, c.isAutoIncrement);
                        insertSchemaStmt.addBatch();
                    }
                    insertSchemaStmt.executeBatch();
                }

                // Keep local mode aligned with DESTINATION mode schema persistence:
                // store source CREATE SQL in table_metadata(key='create_sql').
                String createSql = buildCreateSqlFromColumns(srcTable);
                try (PreparedStatement deleteStmt = conn.prepareStatement(deleteTableMetadataTblSql)) {
                    deleteStmt.setString(1, srcTable.id.database);
                    deleteStmt.setString(2, srcTable.id.table);
                    deleteStmt.setString(3, SyncLiteConsolidatorInfo.getCreateSqlPropKey());
                    deleteStmt.execute();
                }
                try (PreparedStatement insertStmt = conn.prepareStatement(insertTableMetadataTblSql)) {
                    insertStmt.setString(1, srcTable.id.database);
                    insertStmt.setString(2, srcTable.id.schema);
                    insertStmt.setString(3, srcTable.id.table);
                    insertStmt.setString(4, SyncLiteConsolidatorInfo.getCreateSqlPropKey());
                    insertStmt.setString(5, createSql);
                    insertStmt.execute();
                }
                conn.commit();
            }
        }
        // In DESTINATION mode: actual schema persistence to destination is done by DeviceSyncProcessor.persistSchemaToDst()
        this.deviceSrcTables.put(srcTable.id, srcTable);
    }

    private String buildCreateSqlFromColumns(ConsolidatorSrcTable srcTable) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(srcTable.id.table).append(" (");
        ArrayList<Column> pkCols = new ArrayList<Column>();
        boolean first = true;
        for (Column col : srcTable.columns) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(col.column).append(" ").append(col.type.dbNativeDataType);
            if (col.isNotNull != 0) {
                sb.append(" NOT NULL");
            }
            if (col.pkIndex != 0) {
                pkCols.add(col);
            }
            first = false;
        }
        if (pkCols.size() == 1) {
            sb.append(", PRIMARY KEY(").append(pkCols.get(0).column).append(")");
        } else if (pkCols.size() > 1) {
            pkCols.sort((a, b) -> Integer.compare(a.pkIndex, b.pkIndex));
            sb.append(", PRIMARY KEY(");
            for (int i = 0; i < pkCols.size(); ++i) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(pkCols.get(i).column);
            }
            sb.append(")");
        }
        sb.append(")");
        return sb.toString();
    }

    
    public void deleteSchema(ConsolidatorSrcTable srcTable) throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                conn.setAutoCommit(false);
                try (PreparedStatement deleteSchemaStmt = conn.prepareStatement(deleteSchemaTblSql)) {
                    deleteSchemaStmt.setString(1, srcTable.id.database);
                    deleteSchemaStmt.setString(2, srcTable.id.table);
                    deleteSchemaStmt.execute();
                }
                conn.commit();
            }
        }
        // In DESTINATION mode: actual schema deletion from destination is done by DeviceSyncProcessor.deleteSchemaFromDst()
        this.deviceSrcTables.remove(srcTable.id);
    }

    public Collection<ConsolidatorSrcTable> getConsolidatorSrcTables() {
    	return this.deviceSrcTables.values();
    }
    
    public void loadSchemas(Device device) throws SQLException {
        if (isDestinationMetadataMode()) {
            // In DESTINATION mode schemas are loaded from destination via DeviceSyncProcessor.loadSchemasFromDestination()
            // which calls upsertSchema() to populate the in-memory cache. Nothing to do here.
            return;
        }
	this.deviceSrcTables.clear();
        String sql = "SELECT database_name, table_name, value FROM table_metadata WHERE key = '" + SyncLiteConsolidatorInfo.getCreateSqlPropKey() + "' ORDER BY database_name, table_name";
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    try (Connection schemaConn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
                        while (rs.next()) {
                            String dbName = rs.getString(1);
                            String tableName = rs.getString(2);
                            String createSql = rs.getString(3);
                            if (createSql == null || createSql.isBlank()) {
                                continue;
                            }
                            try (Statement schemaStmt = schemaConn.createStatement()) {
                                schemaStmt.execute(createSql);
                            }
                            TableID tableID = TableID.from(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex, dbName, null, tableName);
                            ConsolidatorSrcTable currentTable = ConsolidatorSrcTable.from(tableID);
                            currentTable.sql = createSql;
                            currentTable.clearColumns();
                            for (Column c : device.schemaReader.fetchColumns(schemaConn, tableID)) {
                                currentTable.addColumn(c);
                            }
                            this.deviceSrcTables.put(tableID, currentTable);
                        }
                    } catch (SyncLiteException e) {
                        throw new SQLException("Failed to load schema columns from local create_sql metadata", e);
                    }
                }
            }
        }
    }

    public static ConsolidatorMetadataManager getInstance(Path metadataFilePath, Device d, int dstIndex) throws SQLException {
        if (metadataFilePath == null) {
            return null;
        }
        return (ConsolidatorMetadataManager) metadataMgrs.computeIfAbsent(metadataFilePath, s -> {
            try {
                return new ConsolidatorMetadataManager(s, d, dstIndex);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private boolean isDestinationMetadataMode() {
        String mode = ConfLoader.getInstance().getMetadataStore(dstIndex);
        if (!"DESTINATION".equalsIgnoreCase(mode)) {
            return false;
        }
        // Non-JDBC destinations (MongoDB, Apache Iceberg) cannot store metadata on the destination;
        // fall back to local SQLite storage for those.
        return JDBCConnector.getInstance(dstIndex).isJdbcCapable();
    }

    private volatile boolean dstMetadataTableEnsured = false;
    private volatile boolean dstTableMetadataTableEnsured = false;

    private void ensureDstMetadataTable(SQLExecutor dstExecutor) throws DstExecutionException {
        if (dstMetadataTableEnsured) {
            return;
        }
        device.tracer.debug("[BOOT] Ensuring consolidator metadata table exists on destination");
        dstExecutor.execute(systemTableMapper.mapOper(new CreateTable(dstMetadataSystemTable)));
        dstMetadataTableEnsured = true;
        device.tracer.debug("[BOOT] Consolidator metadata table ensured on destination");
    }

    private volatile boolean dstVersionSeeded = false;

    /**
     * Seed the {@code synclite_metadata_version} row inline on the caller's connection.
     * Uses DELETE+INSERT on the same dstExecutor transaction to avoid a separate
     * SELECT connection which would deadlock on PostgreSQL: the dstExecutor holds an
     * open AccessExclusiveLock from the preceding CREATE TABLE, and a SELECT on a
     * second autoCommit=false connection from the same pool waits indefinitely.
     */
    private void seedDstMetadataVersionIfAbsent(SQLExecutor dstExecutor) throws SQLException, SyncLiteException {
        if (dstVersionSeeded) {
            device.tracer.debug("[BOOT] Metadata version already seeded, skipping");
            return;
        }
        device.tracer.debug("[BOOT] Seeding destination metadata version");
        ensureDstMetadataTable(dstExecutor);
        // Delete-then-insert (idempotent) on the same transaction to avoid opening
        // a second connection for a SELECT check.
        ArrayList<Object> delVals = new ArrayList<Object>(1);
        delVals.add(SYNCLITE_METADATA_VERSION_KEY);
        Delete deleteProp = new Delete(dstMetadataSystemTable, delVals);
        deleteProp.whereColumns = new ArrayList<Column>();
        deleteProp.whereColumns.add(dstMetadataSystemTable.colMap.get("prop_key"));
        dstExecutor.execute(systemTableMapper.mapOper(deleteProp));
        ArrayList<Object> vals = new ArrayList<Object>(2);
        vals.add(SYNCLITE_METADATA_VERSION_KEY);
        vals.add(Long.toString(SYNCLITE_METADATA_VERSION));
        dstExecutor.execute(systemTableMapper.mapOper(new Insert(dstMetadataSystemTable, vals, false)));
        dstVersionSeeded = true;
        device.tracer.info("[BOOT] Destination metadata version seeded successfully");
    }

    private void ensureDstTableMetadataTable(SQLExecutor dstExecutor) throws DstExecutionException {
        if (dstTableMetadataTableEnsured) {
            return;
        }
        dstExecutor.execute(systemTableMapper.mapOper(new CreateTable(dstTableMetadataSystemTable)));
        dstTableMetadataTableEnsured = true;
    }

    /**
     * Returns a schema-qualified reference to a destination system metadata table name
     * using the current destination SQL generator so the quoting style matches the target
     * backend (e.g. MySQL backticks vs PostgreSQL double quotes).
     */
    private String qualifiedDstTableName(String tableName) {
        String schema = ConfLoader.getInstance().getDstSchema(dstIndex);
        if (schema != null && !schema.trim().isEmpty()) {
            return SQLGenerator.getInstance(dstIndex).getSchemaQualifiedObjectName(schema, tableName);
        }
        return tableName;
    }

    @Override
    public void upsertProperties(HashMap<String, Object> values) throws SQLException {
        if (!isDestinationMetadataMode()) {
            super.upsertProperties(values);
            return;
        }
        try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
            dstExecutor.beginTran();
            ensureDstMetadataTable(dstExecutor);
            seedDstMetadataVersionIfAbsent(dstExecutor);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                ArrayList<Object> beforeValues = new ArrayList<Object>(1);
                beforeValues.add(entry.getKey());
                Delete deleteProp = new Delete(dstMetadataSystemTable, beforeValues);
                deleteProp.whereColumns = new ArrayList<Column>();
                deleteProp.whereColumns.add(dstMetadataSystemTable.colMap.get("prop_key"));
                dstExecutor.execute(systemTableMapper.mapOper(deleteProp));

                ArrayList<Object> afterValues = new ArrayList<Object>(2);
                afterValues.add(entry.getKey());
                afterValues.add(entry.getValue() == null ? null : entry.getValue().toString());
                dstExecutor.execute(systemTableMapper.mapOper(new Insert(dstMetadataSystemTable, afterValues, false)));
            }
            dstExecutor.commitTran();
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to connect to destination for consolidator metadata persistence", e);
        }
    }

    /**
     * Boot-time: Insert device metadata property if absent (called once per device boot).
     */
    public void insertPropertyIfAbsent(String key, Object value) throws SQLException {
        if (!isDestinationMetadataMode()) {
            super.upsertProperty(key, value);
            return;
        }

        // Fast read path to avoid unnecessary transaction churn when property already exists.
        if (getStringProperty(key) != null) {
            device.tracer.debug("[BOOT] Device metadata property already exists: " + key);
            return;
        }

        try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
            dstExecutor.beginTran();
            ensureDstMetadataTable(dstExecutor);
            seedDstMetadataVersionIfAbsent(dstExecutor);

            ArrayList<Object> afterValues = new ArrayList<Object>(2);
            afterValues.add(key);
            afterValues.add(value == null ? null : value.toString());
            dstExecutor.execute(systemTableMapper.mapOper(new Insert(dstMetadataSystemTable, afterValues, false)));

            dstExecutor.commitTran();
            device.tracer.info("[BOOT] Device metadata property inserted: " + key + "=" + value);
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to insert device metadata property at boot", e);
        }
    }

    /**
     * Runtime: upsert device metadata property (during segment apply).
     */
    @Override
    public void upsertProperty(String key, Object value) throws SQLException {
        if (!isDestinationMetadataMode()) {
            super.upsertProperty(key, value);
            return;
        }
        try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
            dstExecutor.beginTran();
            ensureDstMetadataTable(dstExecutor);
            seedDstMetadataVersionIfAbsent(dstExecutor);

            ArrayList<Object> beforeValues = new ArrayList<Object>(1);
            beforeValues.add(key);
            Delete deleteProp = new Delete(dstMetadataSystemTable, beforeValues);
            deleteProp.whereColumns = new ArrayList<Column>();
            deleteProp.whereColumns.add(dstMetadataSystemTable.colMap.get("prop_key"));
            dstExecutor.execute(systemTableMapper.mapOper(deleteProp));

            ArrayList<Object> afterValues = new ArrayList<Object>(2);
            afterValues.add(key);
            afterValues.add(value == null ? null : value.toString());
            dstExecutor.execute(systemTableMapper.mapOper(new Insert(dstMetadataSystemTable, afterValues, false)));

            dstExecutor.commitTran();
            device.tracer.debug("[RUNTIME] Device metadata property upserted: " + key + "=" + value);
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to update device metadata property", e);
        }
    }

    @Override
    public void deleteProperty(String key) throws SQLException {
        if (!isDestinationMetadataMode()) {
            super.deleteProperty(key);
            return;
        }
        try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
            dstExecutor.beginTran();
            ensureDstMetadataTable(dstExecutor);
            seedDstMetadataVersionIfAbsent(dstExecutor);

            ArrayList<Object> beforeValues = new ArrayList<Object>(1);
            beforeValues.add(key);
            Delete deleteProp = new Delete(dstMetadataSystemTable, beforeValues);
            deleteProp.whereColumns = new ArrayList<Column>();
            deleteProp.whereColumns.add(dstMetadataSystemTable.colMap.get("prop_key"));
            dstExecutor.execute(systemTableMapper.mapOper(deleteProp));
            dstExecutor.commitTran();
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to connect to destination for consolidator metadata persistence", e);
        }
    }

    @Override
    public String getStringProperty(String key) throws SQLException {
        if (!isDestinationMetadataMode()) {
            return super.getStringProperty(key);
        }
        // Use a single autoCommit=true connection so that:
        // (a) the bootstrap CREATE TABLE IF NOT EXISTS commits immediately, and
        // (b) the subsequent SELECT does not wait for the DDL's AccessExclusiveLock
        //     (which would deadlock when DDL and SELECT are on separate autoCommit=false
        //     connections from the same HikariCP pool on the same thread).
        try (Connection conn = JDBCConnector.getInstance(dstIndex).connect()) {
            conn.setAutoCommit(true);
            String metaTbl = qualifiedDstTableName("synclite_consolidator_metadata");
            if (!dstMetadataTableEnsured) {
                try (Statement ddl = conn.createStatement()) {
                    ddl.execute("CREATE TABLE IF NOT EXISTS " + metaTbl
                            + "(synclite_device_id VARCHAR(64) NOT NULL,"
                            + " synclite_device_name VARCHAR(255) NOT NULL,"
                            + " synclite_update_timestamp TEXT,"
                            + " prop_key VARCHAR(255) NOT NULL,"
                            + " prop_value TEXT,"
                            + " PRIMARY KEY(synclite_device_id, synclite_device_name, prop_key))");
                }
                dstMetadataTableEnsured = true;
            }
            String sql = "SELECT prop_value FROM " + metaTbl + " WHERE synclite_device_id = ? AND synclite_device_name = ? AND prop_key = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, this.device.getDeviceUUID());
                stmt.setString(2, this.device.getDeviceName());
                stmt.setString(3, key);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
            return null;
        } catch (DstExecutionException e) {
            throw new SQLException("Failed to connect to destination for consolidator metadata lookup", e);
        }
    }

    @Override
    public Long getLongProperty(String key) throws SQLException {
        String val = getStringProperty(key);
        if (val == null) {
            return null;
        }
        return Long.valueOf(val);
    }

    public void resetSchemas() throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DELETE FROM schema;");
                }
            }
        } else {
            try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
                dstExecutor.beginTran();
                ensureDstTableMetadataTable(dstExecutor);
                seedDstMetadataVersionIfAbsent(dstExecutor);
                ArrayList<Object> beforeValues = new ArrayList<Object>(1);
                beforeValues.add(SyncLiteConsolidatorInfo.getCreateSqlPropKey());
                Delete deleteSchemaRows = new Delete(dstTableMetadataSystemTable, beforeValues);
                deleteSchemaRows.whereColumns = new ArrayList<Column>();
                deleteSchemaRows.whereColumns.add(dstTableMetadataSystemTable.colMap.get("prop_key"));
                dstExecutor.execute(systemTableMapper.mapOper(deleteSchemaRows));
                dstExecutor.commitTran();
            } catch (SyncLiteException e) {
                throw new SQLException("Failed to connect to destination to reset schemas", e);
            }
        }
        this.deviceSrcTables.clear();
    }

    public void resetTableMetadata() throws SQLException {
        if (!isDestinationMetadataMode()) {
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DELETE FROM table_metadata;");
                }
            }
            return;
        }
        try (SQLExecutor dstExecutor = SQLExecutor.getInstance(device, dstIndex, device.tracer)) {
            dstExecutor.beginTran();
            ensureDstTableMetadataTable(dstExecutor);
            seedDstMetadataVersionIfAbsent(dstExecutor);
            Delete deleteAll = new Delete(dstTableMetadataSystemTable, new ArrayList<Object>());
            deleteAll.whereColumns = new ArrayList<Column>();
            dstExecutor.execute(systemTableMapper.mapOper(deleteAll));
            dstExecutor.commitTran();
        } catch (SyncLiteException e) {
            throw new SQLException("Failed to connect to destination to reset table metadata", e);
        }
    }
    
    
    public final long getConsolidatedSnapshotSize() throws SyncLiteException {
    	try {
    		if (this.consolidatedSnapshotName != null) {
    			Path snapshotPath = device.getDeviceDataRoot().resolve(this.consolidatedSnapshotName);
    			if (Files.exists(snapshotPath)) {
    				return Files.size(snapshotPath);
    			}
    		}
    	} catch(IOException e) {
    		//Not so critical to fail consolidator for this exception.
    		return 0;
    	}
    	return 0;
    }
    
    public final Map<ConsolidatorSrcTable, Long> getInitializedTables() throws SyncLiteException {

        Map<ConsolidatorSrcTable, Long> initializedTables = new HashMap<ConsolidatorSrcTable, Long>(); 
        try {
        	for (ConsolidatorSrcTable tbl : getConsolidatorSrcTables()) {
            Object val = getTableMetadataEntry(tbl, "initial_rows");
            if (val != null) {
                initializedTables.put(tbl, Long.valueOf(val.toString()));
            }
        	}
        } catch (SQLException e) {
            throw new SyncLiteException("Failed to read initialization info from consolidator metadata file : ", e);
        }
        return initializedTables;
    }


    public long getInitializationStatus() {
    	return initializationStatus;
    }
    
    
    public void updateInitializedSnapshotName(String consolidatedSnapshotName) throws SyncLiteException {
        try {
            upsertProperty("initialized_snapshot_name", consolidatedSnapshotName);
            this.consolidatedSnapshotName = consolidatedSnapshotName;
            upsertProperty("initialization_status", 1);
            this.initializationStatus = 1;
            upsertProperty("initialization_count", this.initializationCount + 1);
            this.initializationCount += 1;
        } catch(SQLException e) {
            throw new SyncLiteException("Failed to update consolidator metadata file : ", e);
        }
    }
    
    public void updateLastConsolidatedCDCLogSegmentSeqNum(long seqNum) throws SyncLiteException {
        try {
            upsertProperty("last_consolidated_cdc_log_segment_seq_num", seqNum);
            this.lastConsolidatedCDCLogSegmentSeqNumber = seqNum;
        } catch(SQLException e) {
            throw new SyncLiteException("Failed to update consolidator metadata file : ", e);
        }
    }

    public final void initializeConsolidatorMetadataFile() throws SyncLiteException {
        // For DESTINATION metadata mode, create both system metadata tables and commit
        // them upfront before any property reads open a second connection from the pool.
        // We use a plain autoCommit JDBC connection here instead of SQLExecutor so that:
        // (a) the DDL commits immediately on the same connection, and
        // (b) we avoid SQLExecutor construction overhead during Device.<init> which can
        //     cause connection pool timeouts before the device is registered.
        if (isDestinationMetadataMode()) {
            try (Connection ddlConn = JDBCConnector.getInstance(dstIndex).connect()) {
                ddlConn.setAutoCommit(true);
                try (Statement ddlStmt = ddlConn.createStatement()) {
                    // Build schema-qualified CREATE TABLE IF NOT EXISTS SQL directly so
                    // PostgreSQL creates the tables in the configured schema, not search_path.
                    String metaTbl = qualifiedDstTableName("synclite_consolidator_metadata");
                    String tblMetaTbl = qualifiedDstTableName("synclite_consolidator_table_metadata");
                    ddlStmt.execute("CREATE TABLE IF NOT EXISTS " + metaTbl
                            + "(synclite_device_id VARCHAR(64) NOT NULL,"
                            + " synclite_device_name VARCHAR(255) NOT NULL,"
                            + " synclite_update_timestamp TEXT,"
                            + " prop_key VARCHAR(255) NOT NULL,"
                            + " prop_value TEXT,"
                            + " PRIMARY KEY(synclite_device_id, synclite_device_name, prop_key))");
                    ddlStmt.execute("CREATE TABLE IF NOT EXISTS " + tblMetaTbl
                            + "(synclite_device_id VARCHAR(64) NOT NULL,"
                            + " synclite_device_name VARCHAR(255) NOT NULL,"
                            + " synclite_update_timestamp TEXT,"
                            + " database_name VARCHAR(255) NOT NULL,"
                            + " table_name VARCHAR(255) NOT NULL,"
                            + " prop_key VARCHAR(255) NOT NULL,"
                            + " prop_value TEXT,"
                            + " PRIMARY KEY(synclite_device_id, synclite_device_name, database_name, table_name, prop_key))");
                    // synclite_checkpoint tracks per-device replication progress on the destination.
                    // Bootstrap it here so recovery reads/writes never race with the first
                    // ensureDstMetadataInitStatusColumn call, which runs on an executor thread after
                    // device discovery and would otherwise find the table missing.
                    String checkpointTbl = qualifiedDstTableName("synclite_checkpoint");
                    ddlStmt.execute("CREATE TABLE IF NOT EXISTS " + checkpointTbl
                            + "(synclite_device_id TEXT NOT NULL,"
                            + " synclite_device_name TEXT NOT NULL,"
                            + " synclite_update_timestamp TEXT,"
                            + " commit_id BIGINT NOT NULL,"
                            + " cdc_change_number BIGINT NOT NULL,"
                            + " cdc_log_segment_sequence_number BIGINT NOT NULL,"
                            + " initialization_status INTEGER NOT NULL DEFAULT 0,"
                            + " txn_count BIGINT NOT NULL,"
                            + " PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id))");
                }
                dstMetadataTableEnsured = true;
                dstTableMetadataTableEnsured = true;
            } catch (SQLException e) {
                throw new SyncLiteException("Failed to bootstrap destination metadata tables", e);
            }
        }
        try {
            String strVal = getStringProperty("initialized_snapshot_name");
            if (strVal != null) {
                this.consolidatedSnapshotName = strVal;
            } else {
                this.consolidatedSnapshotName = "";
            }

            Long longVal = getLongProperty("initialization_status");
            if (longVal != null) {
                this.initializationStatus= longVal;
            } else {
                this.initializationStatus= 0;
            }

            longVal = getLongProperty("initialization_count");
            if (longVal != null) {
                this.initializationCount = longVal;
            } else {
                this.initializationCount = 0;
            }
            
            longVal = getLongProperty("last_consolidated_cdc_log_segment_seq_num");
            if (longVal != null) {
            	this.lastConsolidatedCDCLogSegmentSeqNumber = longVal;
            } else {
            	this.lastConsolidatedCDCLogSegmentSeqNumber = -1;
            }            	

        } catch (SQLException e) {
            throw new SyncLiteException("Bad device. Failed to read/write records in metadata file:", e);
        }
        
        if (this.getInitializationStatus() == 1) {
            Monitor.getInstance().incrInitializedDeviceCnt(1L);
        }
        Monitor.getInstance().incrInitializationCnt(this.initializationCount);
        Monitor.getInstance().registerChangedDevice(device);

    }
    
    public void resetInitializedSnapshot() throws SyncLiteException {    	
        try {
    		HashMap<String, Object> values = new HashMap<String, Object>();
    		values.put("initialized_snapshot_name", "");
    		values.put("initialization_status", 0);
            this.consolidatedSnapshotName = "";
            this.initializationStatus = 0;
            upsertProperties(values);
        } catch(SQLException e) {
            throw new SyncLiteException("Failed to update consolidator metadata file : ", e);
        }
    }
  
    public long getLastConsolidatedCDCLogSegmentSeqNum() {
        return this.lastConsolidatedCDCLogSegmentSeqNumber;
    }
   

	public void resetInitializationStatus() throws SyncLiteException {
        try {        	
    		HashMap<String, Object> values = new HashMap<String, Object>();
    		values.put("initialized_snapshot_name", "");
    		values.put("initialization_status", 0);

            this.consolidatedSnapshotName = "";
            this.initializationStatus = 0;
    		upsertProperties(values);
            if (isDestinationMetadataMode()) {
                // Keep destination device status in sync for explicit reinitialize.
                // Otherwise fresh-host recovery may restore status=1 and skip snapshot reinit.
                resetDestinationDeviceStatus();
            }
    		
            deleteSchemaInfo();
            deleteTableMetadataInfo();            
        } catch (SQLException e) {
            throw new SyncLiteException("Failed to reset initializartion status in metadata file : " + this.metadataFilePath, e);
        }

	}

    private void resetDestinationDeviceStatus() throws SQLException {
        try (Connection conn = JDBCConnector.getInstance(dstIndex).connect()) {
            conn.setAutoCommit(false);
            String qcheckpoint = qualifiedDstTableName("synclite_checkpoint");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + qcheckpoint + "("
                        + "synclite_device_id TEXT NOT NULL, "
                        + "synclite_device_name TEXT NOT NULL, "
                        + "synclite_update_timestamp TEXT, "
                        + "commit_id LONG NOT NULL, "
                        + "cdc_change_number LONG NOT NULL, "
                        + "cdc_log_segment_sequence_number LONG NOT NULL, "
                        + "initialization_status INTEGER NOT NULL DEFAULT 0, "
                        + "txn_count LONG NOT NULL, "
                        + "PRIMARY KEY(synclite_device_id, synclite_device_name, commit_id))");
            }
            try (PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE " + qcheckpoint + " SET initialization_status = 0 WHERE synclite_device_id = ? AND synclite_device_name = ?")) {
                updateStmt.setString(1, this.device.getDeviceUUID());
                updateStmt.setString(2, this.device.getDeviceName());
                updateStmt.execute();
            }
            conn.commit();
        } catch (DstExecutionException e) {
            throw new SQLException("Failed to connect to destination to reset device status", e);
        }
    }

	public void executeCheckpointTableSql(String sql) throws SyncLiteException {
		device.tracer.debug("[CHECKPOINT] Executing SQL on LOCAL checkpoint table: " + sql);
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
        	try (Statement stmt = conn.createStatement()) {
        		stmt.execute(sql);
        		device.tracer.debug("[CHECKPOINT] LOCAL checkpoint table SQL executed successfully");
        	}
        } catch(SQLException e) {
        	throw new DstExecutionException("Failed to execute checkpoint table sql : " + e.getMessage(), e);
        }
	}

	public void executeCheckpointTablePreparedStmt(String sql, ArrayList<Object> args) throws SyncLiteException {
		device.tracer.debug("[CHECKPOINT] Executing prepared statement on LOCAL checkpoint table with " + args.size() + " args");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
        	try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
        		int i = 1;
        		for (Object arg : args) {
        			pstmt.setObject(i, arg);
        			++i;
        		}
        		pstmt.execute();
        		device.tracer.debug("[CHECKPOINT] LOCAL checkpoint prepared statement executed successfully");
        	}
        } catch(SQLException e) {
        	throw new DstExecutionException("Failed to execute checkpoint table sql : " + e.getMessage(), e);
        }
	}

	public HashMap<String, Object> readCheckpointRecord(String sql) throws SyncLiteException {
		HashMap<String, Object> result = new HashMap<String, Object>();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
        	try (Statement stmt = conn.createStatement()) {
            	try (ResultSet rs = stmt.executeQuery(sql)) {
            		if (rs.next()) {
            			int colCnt = rs.getMetaData().getColumnCount();
            			for (int i=1; i <=colCnt; ++i) {
            				result.put(rs.getMetaData().getColumnName(i).toLowerCase(), rs.getObject(i));
            			}
            			device.tracer.debug("[READ] Checkpoint record read from LOCAL metadata: " + result);
            		} else {
            			device.tracer.debug("[READ] No checkpoint record found in LOCAL metadata");
            		}
            	}
        	}
        	return result;
        } catch(SQLException e) {
        	throw new SyncLiteException("Failed to read checkpoint table : " + e.getMessage(), e);
        }
	}

	
}

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

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.DstExecutionException;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.schema.Column;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.DataType;
import com.synclite.consolidator.schema.TableID;
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

    protected ConsolidatorMetadataManager(Path metadataFilePath, Device device, int dstIndex) throws SQLException {
        super(metadataFilePath);
        this.device = device;
        this.dstIndex = dstIndex;
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

    public void deleteTableMetadataInfo() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM table_metadata");
            }
        }
    }

    public void deleteSchemaInfo() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM schema");
            }
        }
    }

    public void upsertTableMetadataEntry(ConsolidatorSrcTable srcTable, String key, Object value) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            conn.setAutoCommit(false);
            try (PreparedStatement deleteStmt= conn.prepareStatement(deleteTableMetadataTblSql)) {
                deleteStmt.setString(1, srcTable.id.database);
                deleteStmt.setString(2, srcTable.id.table);
                deleteStmt.setString(3, key);
                deleteStmt.execute();
            }
            try (PreparedStatement insertStmt= conn.prepareStatement(insertTableMetadataTblSql)) {
                insertStmt.setString(1, srcTable.id.database);
                insertStmt.setString(2, srcTable.id.schema);
                insertStmt.setString(3, srcTable.id.table);
                insertStmt.setString(4,key);
                insertStmt.setObject(5,value);
                insertStmt.execute();
            }
            conn.commit();
        }
    }

    public void upsertSchema(ConsolidatorSrcTable srcTable) throws SQLException {
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
            conn.commit();
        }
        
        this.deviceSrcTables.put(srcTable.id, srcTable);
    }

    
    public void deleteSchema(ConsolidatorSrcTable srcTable) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            conn.setAutoCommit(false);
            try (PreparedStatement deleteSchemaStmt = conn.prepareStatement(deleteSchemaTblSql)) {
                deleteSchemaStmt.setString(1, srcTable.id.database);
                deleteSchemaStmt.setString(2, srcTable.id.table);
                deleteSchemaStmt.execute();
            }
            conn.commit();
        }        
        this.deviceSrcTables.remove(srcTable.id);
    }

    public Collection<ConsolidatorSrcTable> getConsolidatorSrcTables() {
    	return this.deviceSrcTables.values();
    }
    
    public void loadSchemas(Device device) throws SQLException {
    	this.deviceSrcTables.clear();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(selectSchemaTblSql)) {
                    TableID currentTableID = null;
                    ConsolidatorSrcTable currentTable = null;
                    while (rs.next()) {
                        TableID tableID = TableID.from(device.getDeviceUUID(), device.getDeviceName(), this.dstIndex, rs.getString(1), rs.getString(2), rs.getString(3));
                        if (!tableID.equals(currentTableID)) {
                            currentTableID = tableID;
                            currentTable = ConsolidatorSrcTable.from(currentTableID);
                            this.deviceSrcTables.put(tableID, currentTable);
                            currentTable.clearColumns();
                        }
                        DataType dataType = new DataType(rs.getString(6), device.schemaReader.getJavaSqlType(rs.getString(6)), device.schemaReader.getStorageClass(rs.getString(6)));
                        Column c = new Column(rs.getLong(4), rs.getString(5), dataType, rs.getInt(7), rs.getString(8), rs.getInt(9), rs.getInt(10));
                        currentTable.addColumn(c);
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

    public void resetSchemas() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM schema;");
            }
        }
        this.deviceSrcTables.clear();
    }

    public void resetTableMetadata() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM table_metadata;");
            }
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
    		
            deleteSchemaInfo();
            deleteTableMetadataInfo();            
        } catch (SQLException e) {
            throw new SyncLiteException("Failed to reset initializartion status in metadata file : " + this.metadataFilePath, e);
        }

	}

	public void executeCheckpointTableSql(String sql) throws SyncLiteException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
        	try (Statement stmt = conn.createStatement()) {
        		stmt.execute(sql);
        	}
        } catch(SQLException e) {
        	throw new DstExecutionException("Failed to execute checkpoint table sql : " + e.getMessage(), e);
        }
	}

	public void executeCheckpointTablePreparedStmt(String sql, ArrayList<Object> args) throws SyncLiteException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metadataFilePath)) {
        	try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
        		int i = 1;
        		for (Object arg : args) {
        			pstmt.setObject(i, arg);
        			++i;
        		}
        		pstmt.execute();
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
            		}
            	}
        	}
        	return result;
        } catch(SQLException e) {
        	throw new SyncLiteException("Failed to read checkpoint table : " + e.getMessage(), e);
        }
	}

	
}

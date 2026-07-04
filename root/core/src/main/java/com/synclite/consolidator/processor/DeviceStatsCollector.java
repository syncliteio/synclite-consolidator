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

package com.synclite.consolidator.processor;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.synclite.consolidator.device.Device;
import com.synclite.consolidator.exception.SyncLiteException;
import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.ConsolidatorMetadataManager;
import com.synclite.consolidator.global.DstSyncMode;
import com.synclite.consolidator.global.SyncLiteConsolidatorInfo;
import com.synclite.consolidator.oper.OperType;
import com.synclite.consolidator.schema.ConsolidatorSrcTable;
import com.synclite.consolidator.schema.TableID;

public class DeviceStatsCollector {

	private Device device;
	private int dstIndex;
	private String dstAlias;
	private long lastStatsCollectedLogSegmentSeqNum = -1;
	private long hasInitializationStatsCollected = 0;
	private final Path statsFilePath;
	private final ConsolidatorMetadataManager consolidatorControlPropMgr;
	private HashSet<TableID> tablesInStats = new HashSet<TableID>();

	private final String createDeviceStatisticsTableSql = "CREATE TABLE IF NOT EXISTS device_statistics(dst_alias TEXT, cdc_log_segment_sequence_number LONG, is_initialization_stats_collected LONG, processed_oper_count LONG, processed_txn_count, processed_log_size LONG)";
	private final String insertDeviceStatisticsTableSql = "INSERT INTO device_statistics(dst_alias, cdc_log_segment_sequence_number, is_initialization_stats_collected, processed_oper_count, processed_txn_count, processed_log_size) VALUES ('$', -1, 0, 0, 0, 0);";
	private final String selectDeviceStatisticsTableSql = "SELECT cdc_log_segment_sequence_number, is_initialization_stats_collected, processed_oper_count, processed_txn_count, processed_log_size FROM device_statistics WHERE dst_alias = '$'";
	private final String updateLogSegmentDeviceStatisticsTableSql = "UPDATE device_statistics SET cdc_log_segment_sequence_number = ?, processed_oper_count = processed_oper_count + ?, processed_txn_count = processed_txn_count + ?, processed_log_size = processed_log_size + ? WHERE dst_alias = ?";
	private final String updateInitializationStatsDeviceStatisticsTableSql = "UPDATE device_statistics SET is_initialization_stats_collected = 1, processed_oper_count = $1, processed_txn_count = $2, processed_log_size = $3 WHERE dst_alias = '$4'";
	private final String resetInitilizationStatsCollectedSql = "UPDATE device_statistics SET is_initialization_stats_collected = 0 WHERE dst_alias = '$'";
	private final String createStatsTableSql = "CREATE TABLE IF NOT EXISTS table_statistics(dst_alias TEXT, database_name TEXT, schema_name TEXT, table_name TEXT, initial_rows LONG, insert_rows LONG, update_rows LONG, delete_rows LONG, add_column LONG, drop_column LONG, rename_column LONG, create_table LONG, drop_table LONG, rename_table LONG, PRIMARY KEY(dst_alias, database_name, schema_name, table_name))";
	private final String insertStatsTableSql = "INSERT OR REPLACE INTO table_statistics(dst_alias, database_name, schema_name, table_name, initial_rows, insert_rows, update_rows, delete_rows, add_column, drop_column, rename_column, create_table, drop_table, rename_table) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private final String updateStatsTableSql = "UPDATE table_statistics SET insert_rows = insert_rows + ?, update_rows = update_rows + ?, delete_rows = delete_rows + ?, add_column = add_column + ?, drop_column = drop_column + ?, rename_column = rename_column + ?, create_table = create_table + ?, drop_table = drop_table + ?, rename_table = rename_table + ? WHERE dst_alias = ? AND database_name = ? AND schema_name = ? AND table_name = ?";
	private final String deleteStatsTableSql = "DELETE FROM table_statistics WHERE dst_alias = ? AND database_name = ? AND schema_name = ? AND table_name = ?";
	private final String selectStatsTableSql = "SELECT database_name, schema_name, table_name FROM table_statistics WHERE dst_alias = '$';";
	private final String deleteAllStatsTableSql = "DELETE FROM table_statistics WHERE dst_alias = '$'";

	public DeviceStatsCollector(Device device, int dstIndex) throws SyncLiteException {
		this.dstIndex = dstIndex;
		this.dstAlias = ConfLoader.getInstance().getDstAlias(this.dstIndex);
		this.device = device;
		this.statsFilePath = device.getStatsFile();
		initStatsFile();
		this.consolidatorControlPropMgr = device.getConsolidatorMetadataMgr(this.dstIndex);
	}

	private void configureSqliteConnection(Connection conn) throws SQLException {
		try (Statement pragmaStmt = conn.createStatement()) {
			pragmaStmt.execute("PRAGMA busy_timeout = 5000");
			pragmaStmt.execute("PRAGMA journal_mode = WAL");
			pragmaStmt.execute("PRAGMA synchronous = NORMAL");
		}
	}

	private void initStatsFile() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(url)) {
			configureSqliteConnection(statsFileConn);
			try (Statement stmt = statsFileConn.createStatement()) {
				stmt.execute(createDeviceStatisticsTableSql);
				try (ResultSet rs = stmt.executeQuery(selectDeviceStatisticsTableSql.replace("$", this.dstAlias))) {
					if (rs.next()) {
						this.lastStatsCollectedLogSegmentSeqNum = rs.getLong(1);
						this.hasInitializationStatsCollected = rs.getLong(2);
						device.incrTotalProcessedLogSegmentCount(this.lastStatsCollectedLogSegmentSeqNum + 1);
						device.incrTotalProcessedOperCount(rs.getLong(3));
						device.incrTotalProcessedTxnCount(rs.getLong(4));
						device.incrTotalProcessedLogSize(rs.getLong(5));
					} else {
						String insertSql = insertDeviceStatisticsTableSql.replace("$", this.dstAlias);
						stmt.execute(insertSql);
						this.lastStatsCollectedLogSegmentSeqNum = -1;
						this.hasInitializationStatsCollected = 0;
					}
				}
				stmt.execute(createStatsTableSql); 
			}

			//Load table list for tables with already existing statistics
			try (Statement stmt = statsFileConn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery(selectStatsTableSql.replace("$", this.dstAlias))) {
					while (rs.next()) {
						String dbName = rs.getString(1);
						String schemaName = rs.getString(2);
						if (schemaName == null) schemaName = "";
						String tableName = rs.getString(3);
						TableID tblID = TableID.from(this.device.getDeviceUUID(), this.device.getDeviceName(), this.dstIndex, dbName, schemaName, tableName);
						tablesInStats.add(tblID);
					}

				}
			}	

		} catch(SQLException e) {
			throw new SyncLiteException("Failed to read initialize stats file for device : " + this.device, e);
		}
	}

	//Method specifically for REPLICATION TO SQLITE usecase 
	protected void updateLogStatsForLogSegment(long logSegmentSeqNumber, long operCount, long txnCount, long logSize) throws SyncLiteException {
		if (this.lastStatsCollectedLogSegmentSeqNum >= logSegmentSeqNumber) {
			return;
		}
		String statsUrl = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(statsUrl);
				PreparedStatement updateLogSegmentDeviceStatisticsTablePstmt = statsFileConn.prepareStatement(updateLogSegmentDeviceStatisticsTableSql);
				Statement statsStmt = statsFileConn.createStatement())
		{
			configureSqliteConnection(statsFileConn);
			updateLogSegmentDeviceStatisticsTablePstmt.setLong(1, logSegmentSeqNumber);
			updateLogSegmentDeviceStatisticsTablePstmt.setLong(2, operCount);
			updateLogSegmentDeviceStatisticsTablePstmt.setLong(3, txnCount);
			updateLogSegmentDeviceStatisticsTablePstmt.setLong(4, logSize);
			updateLogSegmentDeviceStatisticsTablePstmt.setString(5, this.dstAlias);
			updateLogSegmentDeviceStatisticsTablePstmt.execute();
			device.incrTotalProcessedLogSegmentCount(1);
			device.incrTotalProcessedOperCount(operCount);
			device.incrTotalProcessedTxnCount(txnCount);
			device.incrTotalProcessedLogSize(logSize);
			this.lastStatsCollectedLogSegmentSeqNum = logSegmentSeqNumber;

		} catch (SQLException e) {
			throw new SyncLiteException("Failed to update initialization stats in stats file : " + statsFilePath, e);			
		}
	}

	protected void updateTableAndLogStatsForLogSegment(HashMap<TableID, HashMap<OperType, Long>> stats, long logSegmentSeqNumber, long txnCount, long logSize) throws SyncLiteException {
		// Simplified logic: Create missing stats rows first, then update all rows with accumulated operation counts
		if (this.lastStatsCollectedLogSegmentSeqNum >= logSegmentSeqNumber) {
			return;
		}
		String statsUrl = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(statsUrl);
				PreparedStatement insertStatsTablePstmt = statsFileConn.prepareStatement(insertStatsTableSql);
				PreparedStatement updateStatsTablePstmt = statsFileConn.prepareStatement(updateStatsTableSql);
				PreparedStatement updateCDCLogSegmentDeviceStatisticsTablePstmt = statsFileConn.prepareStatement(updateLogSegmentDeviceStatisticsTableSql);
				Statement statsStmt = statsFileConn.createStatement()
				) {			
			configureSqliteConnection(statsFileConn);
			statsFileConn.setAutoCommit(false);
			long totalOperCount = 0;
			HashSet<TableID> newTablesInThisSegment = new HashSet<>();
			
			// Pass 1: Identify new tables and create rows for them
			for (TableID tableID : stats.keySet()) {
				if (!shouldTrackTableStats(tableID) || this.tablesInStats.contains(tableID)) {
					continue;
				}
				
				String schema1 = (tableID.schema != null) ? tableID.schema : "";
				insertStatsTablePstmt.setString(1, this.dstAlias);
				insertStatsTablePstmt.setString(2, tableID.database);
				insertStatsTablePstmt.setString(3, schema1);
				insertStatsTablePstmt.setString(4, tableID.table);
				insertStatsTablePstmt.setLong(5, 0);   // all zero defaults
				insertStatsTablePstmt.setLong(6, 0);
				insertStatsTablePstmt.setLong(7, 0);
				insertStatsTablePstmt.setLong(8, 0);
				insertStatsTablePstmt.setLong(9, 0);
				insertStatsTablePstmt.setLong(10, 0);
				insertStatsTablePstmt.setLong(11, 0);
				insertStatsTablePstmt.setLong(12, 0);
				insertStatsTablePstmt.setLong(13, 0);
				insertStatsTablePstmt.setLong(14, 0);
				insertStatsTablePstmt.addBatch();
				newTablesInThisSegment.add(tableID);
				this.tablesInStats.add(tableID);
			}
			if (!newTablesInThisSegment.isEmpty()) {
				insertStatsTablePstmt.executeBatch();
			}
			
			// Pass 2: Update all tables with accumulated operation counts
			for (HashMap.Entry<TableID, HashMap<OperType, Long>> entry : stats.entrySet()) {
				TableID tableID = entry.getKey();
				if (!shouldTrackTableStats(tableID)) {
					continue;
				}
				
				// Accumulate all operation counts for this table
				Map<OperType, Long> opCounts = entry.getValue();
				long insertCount = 0, updateCount = 0, deleteCount = 0, addColCount = 0, dropColCount = 0, renameColCount = 0, createTableCount = 0, dropTableCount = 0, renameTableCount = 0;
				for (Map.Entry<OperType, Long> opEntry : opCounts.entrySet()) {
					OperType opType = normalizeStatsOperType(opEntry.getKey());
					if (opType == null) {
						continue;
					}
					Long opCount = opEntry.getValue();
					if (opType == OperType.INSERT) insertCount += opCount;
					else if (opType == OperType.UPDATE) updateCount += opCount;
					else if (opType == OperType.DELETE || opType == OperType.DELETE_IF_PREDICATE || opType == OperType.MINUS) deleteCount += opCount;
					else if (opType == OperType.ADDCOLUMN) addColCount += opCount;
					else if (opType == OperType.DROPCOLUMN) dropColCount += opCount;
					else if (opType == OperType.RENAMECOLUMN) renameColCount += opCount;
					else if (opType == OperType.CREATETABLE) createTableCount += opCount;
					else if (opType == OperType.DROPTABLE) dropTableCount += opCount;
					else if (opType == OperType.RENAMETABLE) renameTableCount += opCount;
					totalOperCount += opCount;
				}
				
				// Execute accumulated update
				String schema2 = (tableID.schema != null) ? tableID.schema : "";
				updateStatsTablePstmt.setLong(1, insertCount);
				updateStatsTablePstmt.setLong(2, updateCount);
				updateStatsTablePstmt.setLong(3, deleteCount);
				updateStatsTablePstmt.setLong(4, addColCount);
				updateStatsTablePstmt.setLong(5, dropColCount);
				updateStatsTablePstmt.setLong(6, renameColCount);
				updateStatsTablePstmt.setLong(7, createTableCount);
				updateStatsTablePstmt.setLong(8, dropTableCount);
				updateStatsTablePstmt.setLong(9, renameTableCount);
				updateStatsTablePstmt.setString(10, this.dstAlias);
				updateStatsTablePstmt.setString(11, tableID.database);
				updateStatsTablePstmt.setString(12, schema2);
				updateStatsTablePstmt.setString(13, tableID.table);
				updateStatsTablePstmt.addBatch();
			}
			
			if (!stats.isEmpty()) {
				updateStatsTablePstmt.executeBatch();
			}

			updateCDCLogSegmentDeviceStatisticsTablePstmt.setLong(1, logSegmentSeqNumber);
			updateCDCLogSegmentDeviceStatisticsTablePstmt.setLong(2, totalOperCount);
			updateCDCLogSegmentDeviceStatisticsTablePstmt.setLong(3, txnCount);
			updateCDCLogSegmentDeviceStatisticsTablePstmt.setLong(4, logSize);
			updateCDCLogSegmentDeviceStatisticsTablePstmt.setString(5, this.dstAlias);
			updateCDCLogSegmentDeviceStatisticsTablePstmt.execute();
			statsFileConn.commit();
			statsFileConn.setAutoCommit(true);
			device.incrTotalProcessedLogSegmentCount(1);
			device.incrTotalProcessedOperCount(totalOperCount);
			device.incrTotalProcessedTxnCount(txnCount);
			device.incrTotalProcessedLogSize(logSize);
			this.lastStatsCollectedLogSegmentSeqNum = logSegmentSeqNumber;

		} catch (SQLException e) {
			throw new SyncLiteException("Failed to update initialization stats in stats file : " + statsFilePath, e);			
		}
	}

	protected static TableID resolveStatsTableId(TableID tableId, OperType opType, String oldTableName) {
		if (tableId != null && opType == OperType.RENAMETABLE && oldTableName != null && !oldTableName.isEmpty()) {
			return TableID.from(tableId.deviceUUID, tableId.deviceName, tableId.dstIndex, tableId.database, tableId.schema, oldTableName);
		}
		return tableId;
	}

	private OperType normalizeStatsOperType(OperType opType) {
		if (ConfLoader.getInstance().getDstSyncMode() == DstSyncMode.CONSOLIDATION) {
			if (opType == OperType.DROPTABLE || opType == OperType.DROPCOLUMN) {
				return null;
			}
			if (opType == OperType.RENAMECOLUMN) {
				return OperType.ADDCOLUMN;
			}
			if (opType == OperType.RENAMETABLE) {
				return OperType.CREATETABLE;
			}
		}
		return opType;
	}

	protected void updateInitialTableAndDeviceStatistics() throws SyncLiteException {
		device.tracer.info("Collecting initialization statistics");
		if (hasInitializationStatsCollected == 1) {
			device.tracer.info("initialization statistics already collected, skipping");
			return;
		}
		long totalInitializationRowCount = 0;
		long totalInitializationSnapshotSize = consolidatorControlPropMgr.getConsolidatedSnapshotSize();
		String url = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(url); 
				PreparedStatement insertStatsTablePstmt = statsFileConn.prepareStatement(insertStatsTableSql);
				PreparedStatement deleteStatsTablePstmt = statsFileConn.prepareStatement(deleteStatsTableSql);
				Statement stmt = statsFileConn.createStatement()
				) {

			configureSqliteConnection(statsFileConn);
			statsFileConn.setAutoCommit(false);
			boolean deleteBatchIsFilled = false;
			boolean insertBatchIsFilled = false;
			
			for (Map.Entry<ConsolidatorSrcTable, Long> entry : consolidatorControlPropMgr.getInitializedTables().entrySet()) {
				if (!shouldTrackTableStats(entry.getKey().id)) {
					continue;
				}

				String initSchema = (entry.getKey().id.schema != null) ? entry.getKey().id.schema : "";
				deleteStatsTablePstmt.setString(1, this.dstAlias);
				deleteStatsTablePstmt.setString(2, entry.getKey().id.database);
				deleteStatsTablePstmt.setString(3, initSchema);
				deleteStatsTablePstmt.setString(4, entry.getKey().id.table);
				deleteStatsTablePstmt.addBatch();
				deleteBatchIsFilled = true;
				
				insertStatsTablePstmt.setString(1, this.dstAlias);
				insertStatsTablePstmt.setString(2, entry.getKey().id.database);
				insertStatsTablePstmt.setString(3, initSchema);
				insertStatsTablePstmt.setString(4, entry.getKey().id.table);

				insertStatsTablePstmt.setLong(5, entry.getValue());
				insertStatsTablePstmt.setLong(6, 0);
				insertStatsTablePstmt.setLong(7, 0);
				insertStatsTablePstmt.setLong(8, 0);
				insertStatsTablePstmt.setLong(9, 0);
				insertStatsTablePstmt.setLong(10, 0);
				insertStatsTablePstmt.setLong(11, 0);
				insertStatsTablePstmt.setLong(12, 0);
				insertStatsTablePstmt.setLong(13, 0);
				insertStatsTablePstmt.setLong(14, 0);

				insertStatsTablePstmt.addBatch();
				insertBatchIsFilled = true;
				
				totalInitializationRowCount += entry.getValue();
			}

			if (deleteBatchIsFilled) {
				deleteStatsTablePstmt.executeBatch();
			}
			if (insertBatchIsFilled) {
				insertStatsTablePstmt.executeBatch();
			}
			String updateSql = updateInitializationStatsDeviceStatisticsTableSql.replace("$1", String.valueOf(totalInitializationRowCount));
			updateSql = updateSql.replace("$2", String.valueOf(consolidatorControlPropMgr.getInitializedTables().entrySet().size()));
			updateSql = updateSql.replace("$3", String.valueOf(totalInitializationSnapshotSize));
			updateSql = updateSql.replace("$4", this.dstAlias);
			stmt.execute(updateSql);
			statsFileConn.commit();
			statsFileConn.setAutoCommit(true);
			device.incrTotalProcessedOperCount(totalInitializationRowCount);
			device.incrTotalProcessedTxnCount(consolidatorControlPropMgr.getInitializedTables().entrySet().size());
			device.incrTotalProcessedLogSize(totalInitializationSnapshotSize);
			this.hasInitializationStatsCollected = 1;
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to update initialization stats in stats file : " + statsFilePath, e);
		}
	}

	private boolean shouldTrackTableStats(TableID tableID) {
		return tableID != null && !SyncLiteConsolidatorInfo.getSyncLiteCheckpointTableName().equalsIgnoreCase(tableID.table);
	}

	//Method specifically for REPLICATION TO SQLITE usecase 
	protected void updateInitialDeviceStatistics(long operCount, long txnCount, long size) throws SyncLiteException {
		device.tracer.info("Collecting initial device statistics");
		if (hasInitializationStatsCollected == 1) {
			device.tracer.info("initialization statistics already collected, skipping");
			return;
		}
		String url = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(url); 
				Statement stmt = statsFileConn.createStatement()
				) {			
			configureSqliteConnection(statsFileConn);
			String updateSql = updateInitializationStatsDeviceStatisticsTableSql.replace("$1", String.valueOf(operCount));
			updateSql = updateSql.replace("$2", String.valueOf(txnCount));
			updateSql = updateSql.replace("$3", String.valueOf(size));
			updateSql = updateSql.replace("$4", this.dstAlias);
			stmt.execute(updateSql);
			statsFileConn.commit();
			device.incrTotalProcessedOperCount(operCount);
			device.incrTotalProcessedOperCount(txnCount);
			device.incrTotalProcessedLogSize(size);
			this.hasInitializationStatsCollected = 1;
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to update initialization stats in stats file : " + statsFilePath, e);
		}
	}

	public final void resetTableStats() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(url)) {
			configureSqliteConnection(statsFileConn);
			statsFileConn.setAutoCommit(false);
			try (Statement stmt = statsFileConn.createStatement()) {
				stmt.execute(deleteAllStatsTableSql.replace("$", this.dstAlias));
				stmt.execute(resetInitilizationStatsCollectedSql.replace("$", this.dstAlias));
			}
			statsFileConn.commit();
			statsFileConn.setAutoCommit(true);
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to delete table stats in stats file : " + statsFilePath, e);
		}
	}

}

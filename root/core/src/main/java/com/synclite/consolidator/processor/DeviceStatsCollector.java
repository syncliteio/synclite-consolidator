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

	private final String createCheckpointTableSql = "CREATE TABLE IF NOT EXISTS checkpoint(dst_alias TEXT, cdc_log_segment_sequence_number LONG, is_initialization_stats_collected LONG, processed_oper_count LONG, processed_txn_count, processed_log_size LONG)";
	private final String insertCheckpointTableSql = "INSERT INTO checkpoint(dst_alias, cdc_log_segment_sequence_number, is_initialization_stats_collected, processed_oper_count, processed_txn_count, processed_log_size) VALUES ('$', -1, 0, 0, 0, 0);";
	private final String selectCheckpointTableSql = "SELECT cdc_log_segment_sequence_number, is_initialization_stats_collected, processed_oper_count, processed_txn_count, processed_log_size FROM checkpoint WHERE dst_alias = '$'";
	private final String updateLogSegmentCheckpointTableSql = "UPDATE checkpoint SET cdc_log_segment_sequence_number = ?, processed_oper_count = processed_oper_count + ?, processed_txn_count = processed_txn_count + ?, processed_log_size = processed_log_size + ? WHERE dst_alias = ?";
	private final String updateInitializationStatsCheckpointTableSql = "UPDATE checkpoint SET is_initialization_stats_collected = 1, processed_oper_count = $1, processed_txn_count = $2, processed_log_size = $3 WHERE dst_alias = '$4'";
	private final String resetInitilizationStatsCollectedSql = "UPDATE checkpoint SET is_initialization_stats_collected = 0 WHERE dst_alias = '$'";
	private final String createStatsTableSql = "CREATE TABLE IF NOT EXISTS table_statistics(dst_alias TEXT, database_name TEXT, schema_name TEXT, table_name TEXT, initial_rows LONG, insert_rows LONG, update_rows LONG, delete_rows LONG, add_column LONG, drop_column LONG, rename_column LONG, create_table LONG, drop_table LONG, rename_table LONG, PRIMARY KEY(dst_alias, database_name, schema_name, table_name))";
	private final String insertStatsTableSql = "INSERT OR REPLACE INTO table_statistics(dst_alias, database_name, schema_name, table_name, initial_rows, insert_rows, update_rows, delete_rows, add_column, drop_column, rename_column, create_table, drop_table, rename_table) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private final String updateStatsTableSql = "UPDATE table_statistics SET insert_rows = insert_rows + ?, update_rows = update_rows + ?, delete_rows = delete_rows + ?, add_column = add_column + ?, drop_column = drop_column + ?, rename_column = rename_column + ?, create_table = create_table + ?, drop_table = drop_table + ?, rename_table = rename_table + ? WHERE dst_alias = ? AND database_name = ? AND table_name = ?";
	private final String deleteStatsTableSql = "DELETE FROM table_statistics WHERE dst_alias = ? AND database_name = ? AND table_name = ?";
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

	private void initStatsFile() throws SyncLiteException {
		String url = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(url)) {
			try (Statement stmt = statsFileConn.createStatement()) {
				stmt.execute(createCheckpointTableSql);
				try (ResultSet rs = stmt.executeQuery(selectCheckpointTableSql.replace("$", this.dstAlias))) {
					if (rs.next()) {
						this.lastStatsCollectedLogSegmentSeqNum = rs.getLong(1);
						this.hasInitializationStatsCollected = rs.getLong(2);
						device.incrTotalProcessedLogSegmentCount(this.lastStatsCollectedLogSegmentSeqNum + 1);
						device.incrTotalProcessedOperCount(rs.getLong(3));
						device.incrTotalProcessedTxnCount(rs.getLong(4));
						device.incrTotalProcessedLogSize(rs.getLong(5));			
					} else {
						String insertSql = insertCheckpointTableSql.replace("$", this.dstAlias);
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
						TableID tblID = TableID.from(this.device.getDeviceUUID(), this.device.getDeviceName(), this.dstIndex, rs.getString(1), rs.getString(2), rs.getString(3));
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
				PreparedStatement updateLogSegmentCheckpointTablePstmt = statsFileConn.prepareStatement(updateLogSegmentCheckpointTableSql);
				Statement statsStmt = statsFileConn.createStatement())
		{
			updateLogSegmentCheckpointTablePstmt.setLong(1, logSegmentSeqNumber);
			updateLogSegmentCheckpointTablePstmt.setLong(2, operCount);
			updateLogSegmentCheckpointTablePstmt.setLong(3, txnCount);
			updateLogSegmentCheckpointTablePstmt.setLong(4, logSize);
			updateLogSegmentCheckpointTablePstmt.setString(5, this.dstAlias);
			updateLogSegmentCheckpointTablePstmt.execute();
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
		if (this.lastStatsCollectedLogSegmentSeqNum >= logSegmentSeqNumber) {
			return;
		}
		String statsUrl = "jdbc:sqlite:" + this.statsFilePath;
		try (Connection statsFileConn = DriverManager.getConnection(statsUrl);
				PreparedStatement insertStatsTablePstmt = statsFileConn.prepareStatement(insertStatsTableSql);
				PreparedStatement updateStatsTablePstmt = statsFileConn.prepareStatement(updateStatsTableSql);
				PreparedStatement updateCDCLogSegmentCheckpointTablePstmt = statsFileConn.prepareStatement(updateLogSegmentCheckpointTableSql);
				Statement statsStmt = statsFileConn.createStatement()
				) {			
			statsFileConn.setAutoCommit(false);
			long totalOperCount = 0;
			boolean hasInsertBatch = false;
			boolean hasUpdateBatch = false;
			insertStatsTablePstmt.clearBatch();
			updateStatsTablePstmt.clearBatch();
			for (HashMap.Entry<TableID, HashMap<OperType, Long>> entry : stats.entrySet()) {
				TableID tableID = entry.getKey();
				Map<OperType, Long> opCounts = entry.getValue();
				for (Map.Entry<OperType, Long> opEntry : opCounts.entrySet()) {
					OperType opType = opEntry.getKey();
					Long opCount = opEntry.getValue();

					String database = tableID.database;
					String schema = tableID.schema;
					String table = tableID.table;

					if (((opType == OperType.CREATETABLE) || (opType == OperType.RENAMETABLE)) && 
							(! this.tablesInStats.contains(tableID))
							) {

						insertStatsTablePstmt.setString(1, this.dstAlias);
						insertStatsTablePstmt.setString(2, database);
						insertStatsTablePstmt.setString(3, schema);
						insertStatsTablePstmt.setString(4, table);

						insertStatsTablePstmt.setLong(5, 0);
						insertStatsTablePstmt.setLong(6, 0);
						insertStatsTablePstmt.setLong(7, 0);
						insertStatsTablePstmt.setLong(8, 0);

						insertStatsTablePstmt.setLong(9, 0);
						insertStatsTablePstmt.setLong(10, 0);
						insertStatsTablePstmt.setLong(11, 0);

						insertStatsTablePstmt.setLong(12, opCount);
						insertStatsTablePstmt.setLong(13, 0);
						insertStatsTablePstmt.setLong(14, 0);

						insertStatsTablePstmt.addBatch();
						hasInsertBatch = true;
						totalOperCount += opCount;
					} else {
						updateStatsTablePstmt.setLong(1, 0);
						updateStatsTablePstmt.setLong(2, 0);
						updateStatsTablePstmt.setLong(3, 0);
						updateStatsTablePstmt.setLong(4, 0);
						updateStatsTablePstmt.setLong(5, 0);
						updateStatsTablePstmt.setLong(6, 0);
						updateStatsTablePstmt.setLong(7, 0);
						updateStatsTablePstmt.setLong(8, 0);
						updateStatsTablePstmt.setLong(9, 0);

						updateStatsTablePstmt.setString(10, this.dstAlias);
						updateStatsTablePstmt.setString(11, database);
						updateStatsTablePstmt.setString(12, table);

						if (opType == OperType.INSERT) {
							updateStatsTablePstmt.setLong(1, opCount);
							totalOperCount += opCount;
						} else if (opType == OperType.UPDATE) {
							updateStatsTablePstmt.setLong(2, opCount);
							totalOperCount += opCount;
						} else if (opType == OperType.DELETE) {
							updateStatsTablePstmt.setLong(3, opCount);
							totalOperCount += opCount;
						} else if (opType == OperType.ADDCOLUMN) {
							updateStatsTablePstmt.setLong(4, opCount);
							totalOperCount += opCount;
						} else if (opType == OperType.DROPCOLUMN) {
							updateStatsTablePstmt.setLong(5, opCount);
							totalOperCount += opCount;
						} else if (opType == OperType.RENAMECOLUMN) {
							updateStatsTablePstmt.setLong(6, opCount);					
							totalOperCount += opCount;
						} else if (opType == OperType.CREATETABLE) {
							updateStatsTablePstmt.setLong(7, opCount);
							totalOperCount += opCount;
						} else if (opType == OperType.DROPTABLE) {
							updateStatsTablePstmt.setLong(8, opCount);
							totalOperCount += opCount;
						} else if (opType == OperType.RENAMETABLE) {
							updateStatsTablePstmt.setLong(9, opCount);
							totalOperCount += opCount;
						}
						updateStatsTablePstmt.addBatch();
						hasUpdateBatch = true;
					}					
				}
				this.tablesInStats.add(tableID);
			}
			if (hasInsertBatch) {
				insertStatsTablePstmt.executeBatch();
			}

			if (hasUpdateBatch) {
				updateStatsTablePstmt.executeBatch();
			}

			updateCDCLogSegmentCheckpointTablePstmt.setLong(1, logSegmentSeqNumber);
			updateCDCLogSegmentCheckpointTablePstmt.setLong(2, totalOperCount);
			updateCDCLogSegmentCheckpointTablePstmt.setLong(3, txnCount);
			updateCDCLogSegmentCheckpointTablePstmt.setLong(4, logSize);
			updateCDCLogSegmentCheckpointTablePstmt.setString(5, this.dstAlias);
			updateCDCLogSegmentCheckpointTablePstmt.execute();
			statsFileConn.commit();
			device.incrTotalProcessedLogSegmentCount(1);
			device.incrTotalProcessedOperCount(totalOperCount);
			device.incrTotalProcessedTxnCount(txnCount);
			device.incrTotalProcessedLogSize(logSize);
			this.lastStatsCollectedLogSegmentSeqNum = logSegmentSeqNumber;

		} catch (SQLException e) {
			throw new SyncLiteException("Failed to update initialization stats in stats file : " + statsFilePath, e);			
		}
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

			statsFileConn.setAutoCommit(false);
			boolean deleteBatchIsFilled = false;
			boolean insertBatchIsFilled = false;
			
			for (Map.Entry<ConsolidatorSrcTable, Long> entry : consolidatorControlPropMgr.getInitializedTables().entrySet()) {

				deleteStatsTablePstmt.setString(1, this.dstAlias);
				deleteStatsTablePstmt.setString(2, entry.getKey().id.database);						
				deleteStatsTablePstmt.setString(3, entry.getKey().id.table);
				deleteStatsTablePstmt.addBatch();
				deleteBatchIsFilled = true;
				
				insertStatsTablePstmt.setString(1, this.dstAlias);
				insertStatsTablePstmt.setString(2, entry.getKey().id.database);
				insertStatsTablePstmt.setString(3, entry.getKey().id.schema);
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
			String updateSql = updateInitializationStatsCheckpointTableSql.replace("$1", String.valueOf(totalInitializationRowCount));
			updateSql = updateSql.replace("$2", String.valueOf(consolidatorControlPropMgr.getInitializedTables().entrySet().size()));
			updateSql = updateSql.replace("$3", String.valueOf(totalInitializationSnapshotSize));
			updateSql = updateSql.replace("$4", this.dstAlias);
			stmt.execute(updateSql);
			statsFileConn.commit();
			device.incrTotalProcessedOperCount(totalInitializationRowCount);
			device.incrTotalProcessedTxnCount(consolidatorControlPropMgr.getInitializedTables().entrySet().size());
			device.incrTotalProcessedLogSize(totalInitializationSnapshotSize);
			this.hasInitializationStatsCollected = 1;
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to update initialization stats in stats file : " + statsFilePath, e);
		}
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
			String updateSql = updateInitializationStatsCheckpointTableSql.replace("$1", String.valueOf(operCount));
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
			statsFileConn.setAutoCommit(false);
			try (Statement stmt = statsFileConn.createStatement()) {
				stmt.execute(deleteAllStatsTableSql.replace("$", this.dstAlias));
				stmt.execute(resetInitilizationStatsCollectedSql.replace("$", this.dstAlias));
			}
			statsFileConn.commit();
		} catch (SQLException e) {
			throw new SyncLiteException("Failed to delete table stats in stats file : " + statsFilePath, e);
		}
	}

}
